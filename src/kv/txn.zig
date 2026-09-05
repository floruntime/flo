//! Per-Shard KV Transactions.
//!
//! A transaction is pinned to one shard's partition at BEGIN time. All ops
//! inside the txn must hash to the same partition (cross-shard ops fail fast
//! with `kv_txn_cross_shard`). On COMMIT, the entire write set is appended
//! to the UAL as a single `kv_batch` entry — atomicity is the same as any
//! single Raft propose.
//!
//! ## Lifecycle
//!
//! ```
//! BEGIN(routing_key)        → server pins txn to hash(namespace, routing_key)
//!                             returns txn_id (per-shard, monotonically increasing)
//! PUT/DEL/INCR/...(--txn T) → buffered in TxnState.write_set
//! GET/EXISTS(--txn T)       → checks write_set first (read-your-writes),
//!                             falls back to committed projection state
//! COMMIT(T)                 → propose kv_batch UAL entry, drop TxnState
//! ROLLBACK(T)               → drop TxnState (no UAL write)
//! ```
//!
//! ## Caps
//!
//! - `MAX_OPS_PER_TXN` (256): number of buffered ops
//! - `MAX_PAYLOAD_PER_TXN` (1 MiB): total buffered key+value bytes
//! - `MAX_OPEN_TXNS` (1024): per-shard concurrent txns
//! - `IDLE_TIMEOUT_NS` (5 s): unused — txn idle reaper is follow-up work
//!
//! ## Threading
//!
//! TxnTable is owned by KVProjection (which is owned by a Partition, which
//! lives on one Shard). All access is from the shard's reactor thread. No
//! locks needed.

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;
const stdx_time = @import("stdx").time;

// ─── Caps ─────────────────────────────────────────────────────────────────

pub const MAX_OPS_PER_TXN: u16 = 256;
pub const MAX_PAYLOAD_PER_TXN: usize = 1 * 1024 * 1024;
/// Upper bound of `batchPayloadSize`: the key+value budget plus the fixed
/// framing of every op (kind, flags, namespace, key len, value len, expiry).
pub const MAX_BATCH_ENTRY_PAYLOAD: usize = 3 + MAX_OPS_PER_TXN * 20 + MAX_PAYLOAD_PER_TXN;
pub const MAX_OPEN_TXNS: usize = 1024;

// ─── Op record ────────────────────────────────────────────────────────────

pub const TxnOpKind = enum(u8) {
    put = 1,
    delete = 2,
    incr = 3,
    touch = 4,
    persist = 5,
};

/// One buffered op inside a transaction. Owns the key and value bytes.
pub const TxnOp = struct {
    kind: TxnOpKind,
    /// Namespace-qualified key (already prefixed with `namespace\0`).
    key: []u8,
    /// Op-specific value bytes:
    ///   put     → arbitrary value
    ///   delete  → empty
    ///   incr    → 8-byte i64 LE delta
    ///   touch   → 8-byte u64 LE absolute expiry_ns (0 = clear)
    ///   persist → empty (equivalent to touch with expiry_ns=0)
    value: []u8,
    /// Absolute expiry_ns for puts that carry a TTL. 0 = no TTL.
    /// Only meaningful for `kind == .put`.
    expiry_ns: u64,
};

// ─── Per-txn state ────────────────────────────────────────────────────────

pub const TxnState = struct {
    txn_id: u64,
    /// Wyhash of `namespace \0 routing_key` — the pinned partition routing hash.
    pinned_hash: u64,
    /// Wyhash of the namespace string — every op in the txn must share this.
    /// (v1 limitation: a txn is single-namespace.)
    namespace_hash: u32,
    /// The connection that opened this txn (for cleanup on disconnect).
    /// 0 = anonymous / not bound to a connection (e.g. tests).
    owner_conn_id: u32,
    /// Monotonic timestamp of last activity (ms). For idle reaper.
    last_active_ms: i64,
    /// Buffered ops in submission order.
    ops: std.ArrayListUnmanaged(TxnOp),
    /// Total bytes buffered (sum of key.len + value.len across ops).
    payload_bytes: usize,

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

pub const TxnTable = struct {
    allocator: Allocator,
    /// Active transactions keyed by txn_id.
    map: std.AutoHashMap(u64, TxnState),
    /// Monotonically increasing per-table id source.
    next_id: u64,

    pub fn init(allocator: Allocator) TxnTable {
        return .{
            .allocator = allocator,
            .map = std.AutoHashMap(u64, TxnState).init(allocator),
            .next_id = 1,
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
    pub fn begin(self: *TxnTable, pinned_hash: u64, namespace_hash: u32, owner_conn_id: u32) !u64 {
        if (self.map.count() >= MAX_OPEN_TXNS) return error.TooManyOpenTxns;

        const id = self.next_id;
        self.next_id +%= 1;
        try self.map.put(id, .{
            .txn_id = id,
            .pinned_hash = pinned_hash,
            .namespace_hash = namespace_hash,
            .owner_conn_id = owner_conn_id,
            .last_active_ms = stdx_time.milliTimestamp(),
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
        expiry_ns: u64,
    ) AppendError!void {
        const txn = self.map.getPtr(txn_id) orelse return error.TxnTooLarge; // unreachable in practice — caller checks
        if (txn.ops.items.len >= MAX_OPS_PER_TXN) return error.TxnTooLarge;
        if (txn.payload_bytes + key.len + value.len > MAX_PAYLOAD_PER_TXN) return error.TxnTooLarge;

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
            .expiry_ns = expiry_ns,
        });
        txn.payload_bytes += key.len + value.len;
        txn.last_active_ms = stdx_time.milliTimestamp();
    }

    /// Drop a transaction (called by COMMIT after successful Raft propose,
    /// or by ROLLBACK, or by connection-close cleanup).
    pub fn drop(self: *TxnTable, txn_id: u64) void {
        if (self.map.fetchRemove(txn_id)) |kv| {
            var state = kv.value;
            state.deinit(self.allocator);
        }
    }

    /// Drop every txn owned by the given connection. Called on disconnect.
    /// Txns with `owner_conn_id == 0` are treated as unowned and survive
    /// connection close (used by stateless CLI clients).
    pub fn dropByConnection(self: *TxnTable, conn_id: u32) usize {
        if (conn_id == 0) return 0;
        var to_drop: std.ArrayListUnmanaged(u64) = .empty;
        defer to_drop.deinit(self.allocator);

        var it = self.map.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.owner_conn_id == conn_id) {
                to_drop.append(self.allocator, entry.key_ptr.*) catch break;
            }
        }
        for (to_drop.items) |id| self.drop(id);
        return to_drop.items.len;
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
// kv_batch UAL entry payload codec
// ═══════════════════════════════════════════════════════════════════════════════
//
// One `kv_batch` UAL entry carries a serialized list of TxnOps. The format
// reuses the same shape as `CommandPayload` (namespace_hash + key + value)
// per op so the projection apply path can reuse familiar primitives.
//
// Layout:
//   [version: u8]            // 1 — bump if format changes
//   [op_count: u16 LE]
//   for each op:
//     [kind: u8]             // TxnOpKind
//     [flags: u8]            // bit 0 = HAS_TTL (expiry_ns appended after value)
//     [namespace_hash: u32 LE]
//     [key_len: u16 LE]
//     [val_len: u32 LE]
//     [key bytes]
//     [val bytes]
//     [expiry_ns: u64 LE]    // only if flags & HAS_TTL

pub const BATCH_VERSION: u8 = 1;
pub const FLAG_HAS_TTL: u8 = 0x01;

/// Compute the serialized size of a batch payload for `ops`.
pub fn batchPayloadSize(ops: []const TxnOp) usize {
    var size: usize = 1 + 2; // version + op_count
    for (ops) |op| {
        size += 1 + 1 + 4 + 2 + 4 + op.key.len + op.value.len;
        if (op.kind == .put and op.expiry_ns != 0) size += 8;
    }
    return size;
}

/// Serialize `ops` into `buf`. `namespace_hash` is the same for every op in
/// a single-shard txn (the txn is pinned to one partition). Returns bytes written.
pub fn serializeBatch(buf: []u8, namespace_hash: u32, ops: []const TxnOp) !usize {
    const need = batchPayloadSize(ops);
    if (buf.len < need) return error.BufferTooSmall;
    var off: usize = 0;

    buf[off] = BATCH_VERSION;
    off += 1;
    std.mem.writeInt(u16, buf[off..][0..2], @intCast(ops.len), .little);
    off += 2;

    for (ops) |op| {
        const has_ttl = op.kind == .put and op.expiry_ns != 0;
        buf[off] = @intFromEnum(op.kind);
        off += 1;
        buf[off] = if (has_ttl) FLAG_HAS_TTL else 0;
        off += 1;
        std.mem.writeInt(u32, buf[off..][0..4], namespace_hash, .little);
        off += 4;
        std.mem.writeInt(u16, buf[off..][0..2], @intCast(op.key.len), .little);
        off += 2;
        std.mem.writeInt(u32, buf[off..][0..4], @intCast(op.value.len), .little);
        off += 4;
        @memcpy(buf[off..][0..op.key.len], op.key);
        off += op.key.len;
        @memcpy(buf[off..][0..op.value.len], op.value);
        off += op.value.len;
        if (has_ttl) {
            std.mem.writeInt(u64, buf[off..][0..8], op.expiry_ns, .little);
            off += 8;
        }
    }
    return off;
}

/// One op recovered from a batched UAL entry. Slices borrow into the entry payload.
pub const BatchedOp = struct {
    kind: TxnOpKind,
    namespace_hash: u32,
    key: []const u8,
    value: []const u8,
    /// Absolute expiry_ns. 0 = no TTL. Only meaningful for `.put`.
    expiry_ns: u64,
};

pub const BatchIterator = struct {
    payload: []const u8,
    off: usize,
    remaining: u16,

    pub fn next(self: *BatchIterator) ?BatchedOp {
        if (self.remaining == 0) return null;
        if (self.off + 1 + 1 + 4 + 2 + 4 > self.payload.len) return null;

        const kind_raw = self.payload[self.off];
        self.off += 1;
        const flags = self.payload[self.off];
        self.off += 1;
        const ns_hash = std.mem.readInt(u32, self.payload[self.off..][0..4], .little);
        self.off += 4;
        const key_len = std.mem.readInt(u16, self.payload[self.off..][0..2], .little);
        self.off += 2;
        const val_len = std.mem.readInt(u32, self.payload[self.off..][0..4], .little);
        self.off += 4;

        if (self.off + key_len + val_len > self.payload.len) return null;
        const key = self.payload[self.off .. self.off + key_len];
        self.off += key_len;
        const value = self.payload[self.off .. self.off + val_len];
        self.off += val_len;

        var expiry_ns: u64 = 0;
        if (flags & FLAG_HAS_TTL != 0) {
            if (self.off + 8 > self.payload.len) return null;
            expiry_ns = std.mem.readInt(u64, self.payload[self.off..][0..8], .little);
            self.off += 8;
        }

        const kind: TxnOpKind = std.enums.fromInt(TxnOpKind, kind_raw) orelse return null;
        self.remaining -= 1;
        return .{
            .kind = kind,
            .namespace_hash = ns_hash,
            .key = key,
            .value = value,
            .expiry_ns = expiry_ns,
        };
    }
};

/// Open an iterator over a kv_batch UAL entry payload. Returns null if the
/// payload is malformed at the header.
pub fn iterateBatch(payload: []const u8) ?BatchIterator {
    if (payload.len < 1 + 2) return null;
    if (payload[0] != BATCH_VERSION) {
        log.err("kv_batch: unsupported version {d}", .{payload[0]});
        return null;
    }
    const count = std.mem.readInt(u16, payload[1..3], .little);
    return .{ .payload = payload, .off = 3, .remaining = count };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "TxnTable: begin / appendOp / drop lifecycle" {
    const t = std.testing;
    var tt = TxnTable.init(t.allocator);
    defer tt.deinit();

    const id = try tt.begin(0xDEADBEEF, 0xABCD, 7);
    try t.expectEqual(@as(usize, 1), tt.count());

    try tt.appendOp(id, .put, "default\x00k1", "v1", 0);
    try tt.appendOp(id, .delete, "default\x00k2", "", 0);
    const txn = tt.get(id).?;
    try t.expectEqual(@as(usize, 2), txn.ops.items.len);
    try t.expectEqual(@as(u64, 0xDEADBEEF), txn.pinned_hash);
    try t.expectEqual(@as(u32, 7), txn.owner_conn_id);

    tt.drop(id);
    try t.expectEqual(@as(usize, 0), tt.count());
}

test "TxnTable: lastOpForKey returns most recent buffered op" {
    const t = std.testing;
    var tt = TxnTable.init(t.allocator);
    defer tt.deinit();

    const id = try tt.begin(1, 0, 0);
    try tt.appendOp(id, .put, "ns\x00k", "v1", 0);
    try tt.appendOp(id, .put, "ns\x00other", "x", 0);
    try tt.appendOp(id, .put, "ns\x00k", "v2", 0);

    const op = tt.lastOpForKey(id, "ns\x00k").?;
    try t.expectEqualStrings("v2", op.value);

    try t.expectEqual(@as(?*const TxnOp, null), tt.lastOpForKey(id, "ns\x00missing"));
}

test "TxnTable: enforces MAX_PAYLOAD_PER_TXN" {
    const t = std.testing;
    var tt = TxnTable.init(t.allocator);
    defer tt.deinit();

    const id = try tt.begin(1, 0, 0);
    const big = try t.allocator.alloc(u8, MAX_PAYLOAD_PER_TXN);
    defer t.allocator.free(big);
    @memset(big, 'a');

    // First op fits exactly (key 2 + value MAX-2)
    try tt.appendOp(id, .put, "k1", big[0 .. MAX_PAYLOAD_PER_TXN - 2], 0);

    // Second op pushes over → too large
    try t.expectError(error.TxnTooLarge, tt.appendOp(id, .put, "k2", "more", 0));
}

test "TxnTable: dropByConnection removes only matching txns" {
    const t = std.testing;
    var tt = TxnTable.init(t.allocator);
    defer tt.deinit();

    const a = try tt.begin(1, 0, 100);
    const b = try tt.begin(1, 0, 100);
    const c = try tt.begin(1, 0, 200);
    _ = a;
    _ = b;
    _ = c;

    const dropped = tt.dropByConnection(100);
    try t.expectEqual(@as(usize, 2), dropped);
    try t.expectEqual(@as(usize, 1), tt.count());
}

test "kv_batch: serializeBatch / iterateBatch round-trip" {
    const t = std.testing;

    var ops: [3]TxnOp = .{
        .{ .kind = .put, .key = @constCast("default\x00alpha"), .value = @constCast("hello"), .expiry_ns = 1_000_000_000 },
        .{ .kind = .delete, .key = @constCast("default\x00beta"), .value = @constCast(""), .expiry_ns = 0 },
        .{ .kind = .incr, .key = @constCast("default\x00counter"), .value = @constCast("\x07\x00\x00\x00\x00\x00\x00\x00"), .expiry_ns = 0 },
    };

    const need = batchPayloadSize(&ops);
    const buf = try t.allocator.alloc(u8, need);
    defer t.allocator.free(buf);

    const written = try serializeBatch(buf, 0xCAFEBABE, &ops);
    try t.expectEqual(need, written);

    var it = iterateBatch(buf[0..written]).?;

    const a = it.next().?;
    try t.expectEqual(TxnOpKind.put, a.kind);
    try t.expectEqualStrings("default\x00alpha", a.key);
    try t.expectEqualStrings("hello", a.value);
    try t.expectEqual(@as(u64, 1_000_000_000), a.expiry_ns);
    try t.expectEqual(@as(u32, 0xCAFEBABE), a.namespace_hash);

    const b = it.next().?;
    try t.expectEqual(TxnOpKind.delete, b.kind);
    try t.expectEqualStrings("default\x00beta", b.key);
    try t.expectEqual(@as(u64, 0), b.expiry_ns);

    const c = it.next().?;
    try t.expectEqual(TxnOpKind.incr, c.kind);
    try t.expectEqual(@as(usize, 8), c.value.len);

    try t.expectEqual(@as(?BatchedOp, null), it.next());
}

test "iterateBatch rejects unknown version" {
    const t = std.testing;
    const bad = [_]u8{ 99, 0, 0 };
    try t.expectEqual(@as(?BatchIterator, null), iterateBatch(&bad));
}
