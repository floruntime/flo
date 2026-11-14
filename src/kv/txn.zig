//! Per-Shard KV Transactions.
//!
//! A transaction is pinned at BEGIN time to the routing hash of its key's
//! {tag} (the whole key when it has none), and every key an op inside it
//! names must route the same way (or is refused with `keys_not_colocated`).
//! On COMMIT, the entire write set is appended to the UAL as a single
//! `kv_batch` entry — atomicity is the same as any single Raft propose.
//!
//! ## Lifecycle
//!
//! ```
//! BEGIN(key)                → server pins txn to hash(namespace id, key's {tag})
//!                             returns txn_id (per-shard, monotonically increasing)
//! PUT/DEL/INCR/...(--txn T) → buffered in TxnState.write_set
//! GET/EXISTS(--txn T)       → checks write_set first (read-your-writes),
//!                             falls back to committed projection state
//! COMMIT(T)                 → propose kv_batch UAL entry, drop TxnState
//! ROLLBACK(T)               → drop TxnState (no UAL write)
//! (no request for IDLE_TIMEOUT_MS) → dropped by the shard's reaper
//! ```
//!
//! A txn is named by its `txn_id`, not by the connection that began it: every
//! op carries `--txn T` and the server finds the txn by id, so it outlives its
//! connection on purpose. The CLI depends on this — `kv txn begin`,
//! `kv put --txn T` and `kv commit T` each open a fresh connection — and the
//! SDKs keep a txn by id over a shared, possibly reconnecting client. So a txn
//! is never dropped when a connection closes; the `IDLE_TIMEOUT_MS` reaper
//! (`kvTxnReapTask`, `node/shard.zig:5637`) is the only thing that reclaims one
//! no client is still driving, and `MAX_OPEN_TXNS` bounds how many can be open
//! at once.
//!
//! ## Caps
//!
//! - `TxnTable.max_ops` (`[kv] max_txn_ops`, 64 unless set, at most
//!   `MAX_OPS_PER_TXN`): number of buffered ops. Checked as ops are buffered,
//!   on the leader, and never at apply: replicas with other values must apply
//!   what a leader committed.
//! - `TxnTable.max_bytes` (`[kv] max_txn_bytes`, 256 KiB unless set, at most
//!   `MAX_PAYLOAD_PER_TXN`): total buffered key+value bytes, the same way.
//! - `MAX_OPEN_TXNS` (1024): per-shard concurrent txns
//! - `IDLE_TIMEOUT_MS` (30 s): a txn no request has used for this long is
//!   dropped
//!
//! ## Threading
//!
//! TxnTable is owned by the shard's KVHandler. All access is from the
//! shard's reactor thread. No locks needed.

const std = @import("std");
const NamespaceId = @import("../namespace/table.zig").Id;
const router = @import("../node/router.zig");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;
const stdx_time = @import("stdx").time;
const limits = @import("../protocol/limits.zig");
const kv_ops = @import("../protocol/kv_ops.zig");
const schema = @import("../protocol/schema.zig");
const kv_entries = @import("../protocol/kv_entries.zig");

// ─── Caps ─────────────────────────────────────────────────────────────────

pub const MAX_OPS_PER_TXN = limits.MAX_OPS_PER_TXN;
pub const MAX_PAYLOAD_PER_TXN = limits.MAX_PAYLOAD_PER_TXN;
pub const MAX_OPEN_TXNS: usize = 1024;
/// A transaction isn't tied to a connection: a shell script may open it,
/// write in it and commit it from separate `flo` runs. So the only sign its
/// client is gone is that it stops being used, and without a limit a client
/// that crashed mid-transaction would hold one of `MAX_OPEN_TXNS` for good.
/// Long enough for a script's pauses; a txn reaped under one is answered
/// no_such_transaction and nothing of it is written.
pub const IDLE_TIMEOUT_MS: u64 = 30 * std.time.ms_per_s;

// ─── Op record ────────────────────────────────────────────────────────────

pub const TxnOpKind = kv_entries.BatchKind;

/// One buffered op inside a transaction. Owns the key and value bytes.
pub const TxnOp = struct {
    kind: TxnOpKind,
    /// The key as the client named it: every op is in its transaction's
    /// namespace (`TxnState.ns_id`), which the batch names once.
    key: []u8,
    /// Op-specific value bytes:
    ///   put    → arbitrary value
    ///   delete → empty
    ///   incr   → 8-byte i64 LE delta
    ///   expire → empty
    value: []u8,
    /// TTL in milliseconds, counted from the batch's stamp at apply: a
    /// put's (0 = none) or an expire's (0 = clear it).
    ttl_ms: u64,
    /// Decided when the batch applies, seeing the ops before this one.
    condition: kv_ops.Condition = .always,
    cas_version: ?u64 = null,
};

// ─── Per-txn state ────────────────────────────────────────────────────────

pub const TxnState = struct {
    txn_id: u64,
    /// The routing hash of the key it was begun with: the namespace's id
    /// and the key's {tag}, or the whole key (`router.routeNsKey`).
    pinned_hash: u64,
    /// The namespace the txn was begun in; it is found only from there.
    ns_id: NamespaceId,
    /// When a request last used it, on the monotonic clock (ms).
    last_active_ms: u64,
    /// Buffered ops in submission order.
    ops: std.ArrayListUnmanaged(TxnOp),
    /// Total bytes buffered (sum of key.len + value.len across ops).
    payload_bytes: usize,
    /// Its batch is proposed and hasn't applied yet. It takes no more ops,
    /// and no second commit or rollback: the batch is already in the log.
    committing: bool = false,

    fn deinit(self: *TxnState, allocator: Allocator) void {
        for (self.ops.items) |op| {
            if (op.key.len > 0) allocator.free(op.key);
            if (op.value.len > 0) allocator.free(op.value);
        }
        self.ops.deinit(allocator);
    }
};

pub const AppendError = error{
    TxnTooLarge,
    OutOfMemory,
};

// ─── TxnTable ─────────────────────────────────────────────────────────────

/// The slot a transaction id names: its top 16 bits hold the slot of the
/// hash the transaction is pinned to, so a request inside it routes as a
/// request on its keys does, to the group holding it. The rest count the
/// group's transactions.
///
/// Ids stay unique per slot without naming a group: a group's counter
/// starts at random (`randomCounter`). If a slot moves, its open
/// transactions are dropped at the move's freeze, and the new owner's
/// counter starts at random too, so an id from before matching a new
/// transaction there is a chance of one in 2^48, not a reuse.
pub fn idSlot(id: u64) u32 {
    return @intCast(id >> 48);
}

pub fn makeId(slot: u32, counter: u64) u64 {
    return (@as(u64, slot) << 48) | (counter & ID_COUNTER_MASK);
}

pub const ID_COUNTER_MASK: u64 = (1 << 48) - 1;

/// A counter start no earlier table is likely to have used: 48 random bits,
/// never 0.
fn randomCounter() u64 {
    var bytes: [8]u8 = undefined;
    switch (builtin.os.tag) {
        .linux => {
            var got: usize = 0;
            while (got < bytes.len) {
                const n = std.os.linux.getrandom(bytes[got..].ptr, bytes.len - got, 0);
                switch (std.os.linux.errno(n)) {
                    .SUCCESS => got += n,
                    .INTR => {},
                    // No getrandom: the clock still differs from any
                    // earlier start.
                    else => return @max(1, @as(u64, @truncate(@as(u128, @bitCast(stdx_time.nanoTimestamp())))) & ID_COUNTER_MASK),
                }
            }
        },
        else => std.c.arc4random_buf(&bytes, bytes.len),
    }
    return @max(1, std.mem.readInt(u64, &bytes, .little) & ID_COUNTER_MASK);
}

pub const TxnTable = struct {
    allocator: Allocator,
    max_ops: u16 = limits.DEFAULT_MAX_OPS_PER_TXN,
    max_bytes: usize = limits.DEFAULT_MAX_BYTES_PER_TXN,
    /// Active transactions keyed by txn_id.
    map: std.AutoHashMap(u64, TxnState),
    /// Counts this table's transactions: the low bits of their ids. It
    /// starts at random: the table is in memory, so a restart or a new
    /// leader starts it again, and a client still holding an id from
    /// before must not reach a new transaction with it.
    next_id: u64,

    pub fn init(allocator: Allocator) TxnTable {
        return .{
            .allocator = allocator,
            .map = std.AutoHashMap(u64, TxnState).init(allocator),
            .next_id = randomCounter(),
        };
    }

    pub fn deinit(self: *TxnTable) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.map.deinit();
    }

    /// Begin a new transaction pinned to `pinned_hash`. Returns the new id.
    /// Returns `error.TooManyOpenTxns` when the cap is reached.
    pub fn begin(self: *TxnTable, pinned_hash: u64, ns_id: NamespaceId) !u64 {
        if (self.map.count() >= MAX_OPEN_TXNS) return error.TooManyOpenTxns;

        const id = makeId(router.slot(pinned_hash), self.next_id);
        // Never 0, so no id is all zeros below its slot.
        self.next_id = @max(1, (self.next_id + 1) & ID_COUNTER_MASK);
        try self.map.put(id, .{
            .txn_id = id,
            .pinned_hash = pinned_hash,
            .ns_id = ns_id,
            .last_active_ms = stdx_time.monotonicMs(),
            .ops = .empty,
            .payload_bytes = 0,
        });
        return id;
    }

    pub fn get(self: *TxnTable, txn_id: u64) ?*TxnState {
        return self.map.getPtr(txn_id);
    }

    /// Append an op to the txn. The TxnTable copies `key` and `value` into
    /// allocator-owned buffers.
    pub fn appendOp(
        self: *TxnTable,
        txn_id: u64,
        kind: TxnOpKind,
        key: []const u8,
        value: []const u8,
        ttl_ms: u64,
        condition: kv_ops.Condition,
        cas_version: ?u64,
    ) AppendError!void {
        const txn = self.map.getPtr(txn_id) orelse return error.TxnTooLarge; // unreachable in practice — caller checks
        if (txn.ops.items.len >= self.max_ops) return error.TxnTooLarge;
        if (txn.payload_bytes + key.len + value.len > self.max_bytes) return error.TxnTooLarge;

        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_val = if (value.len == 0)
            try self.allocator.alloc(u8, 0)
        else
            try self.allocator.dupe(u8, value);
        errdefer if (owned_val.len > 0) self.allocator.free(owned_val);

        try txn.ops.append(self.allocator, .{
            .kind = kind,
            .key = owned_key,
            .value = owned_val,
            .ttl_ms = ttl_ms,
            .condition = condition,
            .cas_version = cas_version,
        });
        txn.payload_bytes += key.len + value.len;
    }

    /// Drop a transaction (called by COMMIT after successful Raft propose,
    /// by ROLLBACK, or by the idle reaper).
    pub fn drop(self: *TxnTable, txn_id: u64) void {
        if (self.map.fetchRemove(txn_id)) |kv| {
            var state = kv.value;
            state.deinit(self.allocator);
        }
    }

    /// Drop every transaction no request has used for `IDLE_TIMEOUT_MS` as
    /// of `now_ms`, except one whose batch is proposed: its apply drops it.
    /// Returns how many it dropped.
    pub fn reapIdle(self: *TxnTable, now_ms: u64) usize {
        var reaped: usize = 0;
        // Ids are gathered before dropping: the map can't change under its
        // iterator.
        var ids: [64]u64 = undefined;
        while (true) {
            var n: usize = 0;
            var it = self.map.iterator();
            while (it.next()) |e| {
                const t = e.value_ptr;
                if (t.committing or now_ms -| t.last_active_ms < IDLE_TIMEOUT_MS) continue;
                ids[n] = t.txn_id;
                n += 1;
                if (n == ids.len) break;
            }
            for (ids[0..n]) |id| self.drop(id);
            reaped += n;
            if (n < ids.len) return reaped;
        }
    }

    /// Number of currently open transactions.
    pub fn count(self: *const TxnTable) usize {
        return self.map.count();
    }

    /// Look up the most recent buffered op for `qkey` in this txn, if any.
    /// Used for read-your-writes inside the transaction.
    /// Returns null if the key has no buffered op.
    pub fn lastOpForKey(self: *TxnTable, txn_id: u64, qkey: []const u8) ?*const TxnOp {
        const txn = self.map.getPtr(txn_id) orelse return null;
        var i: usize = txn.ops.items.len;
        while (i > 0) {
            i -= 1;
            const op = &txn.ops.items[i];
            if (std.mem.eql(u8, op.key, qkey)) return op;
        }
        return null;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// The commit's log entry
// ═══════════════════════════════════════════════════════════════════════════════

/// A commit's `kv.batch` payload for `ops`, allocated. An op that breaks
/// its kind's rule is a bug in buffering, refused here rather than logged.
pub fn encodeBatch(allocator: Allocator, ns: NamespaceId, ops: []const TxnOp) (error{ OutOfMemory, InvalidBatch } || schema.EncodeError)![]u8 {
    const values = try allocator.alloc(kv_entries.BatchOp.Value, ops.len);
    defer allocator.free(values);
    for (ops, values) |op, *v| {
        v.* = batchOp(op);
        if (kv_entries.batchOpRule(v.*) != null) return error.InvalidBatch;
    }
    var counter: schema.Writer = .{ .buf = &.{}, .counting = true };
    try kv_entries.encodeBatch(&counter, ns.int(), values);
    const buf = try allocator.alloc(u8, counter.pos);
    errdefer allocator.free(buf);
    var w: schema.Writer = .{ .buf = buf };
    try kv_entries.encodeBatch(&w, ns.int(), values);
    return buf;
}

fn batchOp(op: TxnOp) kv_entries.BatchOp.Value {
    var v: kv_entries.BatchOp.Value = .{ .kind = op.kind, .key = op.key, .value = null, .ttl_ms = null, .delta = null, .condition = op.condition, .cas_version = op.cas_version };
    switch (op.kind) {
        .put => {
            v.value = op.value;
            if (op.ttl_ms != 0) v.ttl_ms = op.ttl_ms;
        },
        .delete => {},
        .incr => v.delta = if (op.value.len == 8) std.mem.readInt(i64, op.value[0..8], .little) else null,
        .expire => v.ttl_ms = op.ttl_ms,
    }
    return v;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "TxnTable: two tables start their ids at different counters" {
    var a = TxnTable.init(std.testing.allocator);
    defer a.deinit();
    var b = TxnTable.init(std.testing.allocator);
    defer b.deinit();
    const first = try a.begin(0, .default);
    // A restart's table: the id a client held from before isn't open in it.
    try std.testing.expect(b.get(first) == null);
    try std.testing.expect(try b.begin(0, .default) != first);
    try std.testing.expect(first & ID_COUNTER_MASK != 0);
}

test "TxnTable: begin / appendOp / drop lifecycle" {
    const t = std.testing;
    var tt = TxnTable.init(t.allocator);
    defer tt.deinit();

    const id = try tt.begin(0xDEADBEEF, .of(0xABCD));
    try t.expectEqual(@as(usize, 1), tt.count());

    try tt.appendOp(id, .put, "k1", "v1", 0, .always, null);
    try tt.appendOp(id, .delete, "k2", "", 0, .always, null);
    const txn = tt.get(id).?;
    try t.expectEqual(@as(usize, 2), txn.ops.items.len);
    try t.expectEqual(@as(u64, 0xDEADBEEF), txn.pinned_hash);

    tt.drop(id);
    try t.expectEqual(@as(usize, 0), tt.count());
}

test "TxnTable: lastOpForKey returns most recent buffered op" {
    const t = std.testing;
    var tt = TxnTable.init(t.allocator);
    defer tt.deinit();

    const id = try tt.begin(1, .default);
    try tt.appendOp(id, .put, "k", "v1", 0, .always, null);
    try tt.appendOp(id, .put, "other", "x", 0, .always, null);
    try tt.appendOp(id, .put, "k", "v2", 0, .always, null);

    const op = tt.lastOpForKey(id, "k").?;
    try t.expectEqualStrings("v2", op.value);

    try t.expectEqual(@as(?*const TxnOp, null), tt.lastOpForKey(id, "missing"));
}

test "TxnTable: by default a transaction holds the largest value a plain set takes, under the longest key" {
    const t = std.testing;
    comptime std.debug.assert(limits.MAX_QUALIFIED_KEY + @import("../protocol/kv_ops.zig").MAX_VALUE <= limits.DEFAULT_MAX_BYTES_PER_TXN);
    var tt = TxnTable.init(t.allocator);
    defer tt.deinit();
    const id = try tt.begin(1, .default);
    const key = try t.allocator.alloc(u8, limits.MAX_QUALIFIED_KEY);
    defer t.allocator.free(key);
    @memset(key, 'k');
    const value = try t.allocator.alloc(u8, @import("../protocol/kv_ops.zig").MAX_VALUE);
    defer t.allocator.free(value);
    @memset(value, 'v');
    try tt.appendOp(id, .put, key, value, 0, .always, null);
}

test "TxnTable: max_bytes and max_ops move their refusal points" {
    const t = std.testing;
    var tt = TxnTable.init(t.allocator);
    defer tt.deinit();
    tt.max_bytes = 10;
    const a = try tt.begin(1, .default);
    try tt.appendOp(a, .put, "k", "123456789", 0, .always, null);
    // One byte over.
    try t.expectError(error.TxnTooLarge, tt.appendOp(a, .put, "k", "", 0, .always, null));
    tt.max_bytes = limits.MAX_PAYLOAD_PER_TXN;
    tt.max_ops = 2;
    const b = try tt.begin(2, .default);
    try tt.appendOp(b, .put, "a", "", 0, .always, null);
    try tt.appendOp(b, .put, "b", "", 0, .always, null);
    try t.expectError(error.TxnTooLarge, tt.appendOp(b, .put, "c", "", 0, .always, null));
}

test "TxnTable: enforces MAX_PAYLOAD_PER_TXN" {
    const t = std.testing;
    var tt = TxnTable.init(t.allocator);
    defer tt.deinit();
    tt.max_bytes = MAX_PAYLOAD_PER_TXN;

    const id = try tt.begin(1, .default);
    const big = try t.allocator.alloc(u8, MAX_PAYLOAD_PER_TXN);
    defer t.allocator.free(big);
    @memset(big, 'a');

    // First op fits exactly (key 2 + value MAX-2)
    try tt.appendOp(id, .put, "k1", big[0 .. MAX_PAYLOAD_PER_TXN - 2], 0, .always, null);

    // Second op pushes over → too large
    try t.expectError(error.TxnTooLarge, tt.appendOp(id, .put, "k2", "more", 0, .always, null));
}

test "TxnTable: reapIdle drops idle transactions, but not a used or committing one" {
    const t = std.testing;
    var tt = TxnTable.init(t.allocator);
    defer tt.deinit();

    // More idle ones than one gathering pass holds.
    var idle: [150]u64 = undefined;
    for (&idle) |*id| {
        id.* = try tt.begin(1, .default);
        tt.get(id.*).?.last_active_ms = 1_000;
    }
    try tt.appendOp(idle[0], .put, "k", "v", 0, .always, null);
    const committing = try tt.begin(1, .default);
    tt.get(committing).?.last_active_ms = 1_000;
    tt.get(committing).?.committing = true;
    const used = try tt.begin(1, .default);
    tt.get(used).?.last_active_ms = 1_000 + IDLE_TIMEOUT_MS;

    const now = 1_000 + IDLE_TIMEOUT_MS;
    try t.expectEqual(@as(usize, 0), tt.reapIdle(now - 1));
    try t.expectEqual(idle.len, tt.reapIdle(now));
    for (idle) |id| try t.expect(tt.get(id) == null);
    try t.expect(tt.get(committing) != null);
    try t.expect(tt.get(used) != null);
    try t.expectEqual(@as(usize, 2), tt.count());
}

test "kv_batch: a commit's ops decode from its payload as they were buffered" {
    const t = std.testing;
    var ops: [4]TxnOp = .{
        .{ .kind = .put, .key = @constCast("alpha"), .value = @constCast("hello"), .ttl_ms = 1_000 },
        .{ .kind = .delete, .key = @constCast("beta"), .value = @constCast(""), .ttl_ms = 0 },
        .{ .kind = .incr, .key = @constCast("counter"), .value = @constCast("\xf9\xff\xff\xff\xff\xff\xff\xff"), .ttl_ms = 0 },
        .{ .kind = .expire, .key = @constCast("gamma"), .value = @constCast(""), .ttl_ms = 0 },
    };
    const payload = try encodeBatch(t.allocator, .of(17), &ops);
    defer t.allocator.free(payload);
    var diag: schema.Diagnostic = .{};
    const batch = try kv_entries.Batch.decodeAll(payload, &diag);
    try t.expectEqual(@as(u32, 17), batch.ns);
    var it = batch.ops.iterator();
    const a = (try it.next()).?;
    try t.expectEqual(TxnOpKind.put, a.kind);
    try t.expectEqualStrings("alpha", a.key);
    try t.expectEqualStrings("hello", a.value.?);
    try t.expectEqual(@as(?u64, 1_000), a.ttl_ms);
    const b = (try it.next()).?;
    try t.expectEqual(TxnOpKind.delete, b.kind);
    try t.expectEqual(@as(?[]const u8, null), b.value);
    const c = (try it.next()).?;
    try t.expectEqual(@as(?i64, -7), c.delta);
    const d = (try it.next()).?;
    try t.expectEqual(TxnOpKind.expire, d.kind);
    try t.expectEqual(@as(?u64, 0), d.ttl_ms);
    try t.expectEqual(@as(?kv_entries.BatchOp.Value, null), try it.next());
}

test "kv_batch: an op that breaks its kind's rule is refused, not logged" {
    var ops = [_]TxnOp{.{ .kind = .incr, .key = @constCast("k"), .value = @constCast(""), .ttl_ms = 0 }};
    try std.testing.expectError(error.InvalidBatch, encodeBatch(std.testing.allocator, .default, &ops));
}

test "txn: an id names the slot of the hash it is pinned to, not a group" {
    const id = makeId(3, 42);
    try std.testing.expectEqual(@as(u32, 3), idSlot(id));
    try std.testing.expectEqual(@as(u64, 42), id & ID_COUNTER_MASK);
    try std.testing.expectEqual(@as(u32, router.SLOTS - 1), idSlot(makeId(router.SLOTS - 1, ID_COUNTER_MASK + 5)));

    var t = TxnTable.init(std.testing.allocator);
    defer t.deinit();
    const pinned = router.routeNsKey(.default, "{user:1}:balance");
    try std.testing.expectEqual(router.slot(pinned), idSlot(try t.begin(pinned, .default)));
    try std.testing.expectEqual(router.nsKeySlot(.default, "{user:1}:name"), idSlot(try t.begin(pinned, .default)));
}
