//! Integration: per-shard KV transactions (batched commit path).
//!
//! Covers the in-memory side of the BEGIN→stage-ops→COMMIT cycle:
//!   1. TxnTable: open, append several mixed ops, drop.
//!   2. Codec: round-trip a batch payload (serialize → iterate).
//!   3. Projection: apply a synthetic kv_batch UAL entry and verify state.

const std = @import("std");
const testing = std.testing;
const src = @import("src");

const txn_mod = src.kv.txn;
const KVProjection = src.projection.kv.KVProjection;
const ual_entry = src.storage.ual.entry;

test "integration: kv_txn — table appends ops and drops cleanly" {
    var table = txn_mod.TxnTable.init(testing.allocator);
    defer table.deinit();

    const tid = try table.begin(0xDEAD_BEEF, 42, 7);
    try testing.expect(tid != 0);

    // Stage put + delete + incr.
    try table.appendOp(tid, .put, "alice", "100", 0);
    try table.appendOp(tid, .delete, "bob", "", 0);

    var delta_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &delta_buf, 5, .little);
    try table.appendOp(tid, .incr, "counter", &delta_buf, 0);

    const state = table.get(tid).?;
    try testing.expectEqual(@as(usize, 3), state.ops.items.len);
    try testing.expectEqual(@as(u64, 0xDEAD_BEEF), state.pinned_hash);
    try testing.expectEqual(@as(u32, 42), state.namespace_hash);
    try testing.expectEqual(@as(u32, 7), state.owner_conn_id);

    // Last-op-for-key lookup powers RYW reads.
    try testing.expect(table.lastOpForKey(tid, "alice") != null);
    try testing.expectEqual(txn_mod.TxnOpKind.put, table.lastOpForKey(tid, "alice").?.kind);
    try testing.expectEqual(txn_mod.TxnOpKind.delete, table.lastOpForKey(tid, "bob").?.kind);
    try testing.expect(table.lastOpForKey(tid, "missing") == null);

    table.drop(tid);
    try testing.expect(table.get(tid) == null);
}

test "integration: kv_txn — connection cleanup drops owned txns" {
    var table = txn_mod.TxnTable.init(testing.allocator);
    defer table.deinit();

    const owner: u32 = 100;
    _ = try table.begin(1, 0, owner);
    _ = try table.begin(2, 0, owner);
    _ = try table.begin(3, 0, 999); // different owner

    try testing.expectEqual(@as(usize, 3), table.count());

    const dropped = table.dropByConnection(owner);
    try testing.expectEqual(@as(usize, 2), dropped);
    try testing.expectEqual(@as(usize, 1), table.count());
}

test "integration: kv_txn — batch payload round-trips through codec" {
    var table = txn_mod.TxnTable.init(testing.allocator);
    defer table.deinit();

    const tid = try table.begin(0, 99, 1);
    try table.appendOp(tid, .put, "k1", "v1", 1_000_000);
    try table.appendOp(tid, .put, "k2", "v2", 0);
    try table.appendOp(tid, .delete, "k3", "", 0);

    const state = table.get(tid).?;
    const size = txn_mod.batchPayloadSize(state.ops.items);
    const buf = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(buf);
    const written = try txn_mod.serializeBatch(buf, state.namespace_hash, state.ops.items);
    try testing.expectEqual(size, written);

    var it = txn_mod.iterateBatch(buf[0..written]).?;
    var seen: usize = 0;
    while (it.next()) |op| : (seen += 1) {
        try testing.expectEqual(@as(u32, 99), op.namespace_hash);
        switch (seen) {
            0 => {
                try testing.expectEqual(txn_mod.TxnOpKind.put, op.kind);
                try testing.expectEqualStrings("k1", op.key);
                try testing.expectEqualStrings("v1", op.value);
                try testing.expectEqual(@as(u64, 1_000_000), op.expiry_ns);
            },
            1 => {
                try testing.expectEqual(txn_mod.TxnOpKind.put, op.kind);
                try testing.expectEqualStrings("k2", op.key);
                try testing.expectEqual(@as(u64, 0), op.expiry_ns);
            },
            2 => {
                try testing.expectEqual(txn_mod.TxnOpKind.delete, op.kind);
                try testing.expectEqualStrings("k3", op.key);
            },
            else => unreachable,
        }
    }
    try testing.expectEqual(@as(usize, 3), seen);
}

test "integration: kv_txn — projection applies kv_batch atomically" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    // Pre-existing state: a key that will be deleted, and a counter to incr.
    try kv.put("doomed", "old", 1, 1, 0, 0);
    try kv.put("counter", &counterBytes(10), 2, 1, 0, 0);

    // Stage a 4-op batch: put, delete, incr, put-with-ttl.
    var ops: [4]txn_mod.TxnOp = undefined;
    var delta_value: [8]u8 = undefined;
    std.mem.writeInt(i64, &delta_value, 7, .little);

    ops[0] = .{ .kind = .put, .key = @constCast("alice"), .value = @constCast("100"), .expiry_ns = 0 };
    ops[1] = .{ .kind = .delete, .key = @constCast("doomed"), .value = @constCast(""), .expiry_ns = 0 };
    ops[2] = .{ .kind = .incr, .key = @constCast("counter"), .value = &delta_value, .expiry_ns = 0 };
    // Far-future absolute ns timestamp (~year 2100).
    const future_ns: u64 = 4_102_444_800_000_000_000;
    ops[3] = .{ .kind = .put, .key = @constCast("ephemeral"), .value = @constCast("temp"), .expiry_ns = future_ns };

    const size = txn_mod.batchPayloadSize(&ops);
    const payload = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(payload);
    _ = try txn_mod.serializeBatch(payload, 0, &ops);

    const entry = ual_entry.buildEntry(.kv_batch, 0, 1, 100, 12345, payload);
    try kv.applyEntry(&entry);

    // Verify final state.
    try testing.expect(kv.get("doomed") == null); // deleted
    {
        const e = kv.get("alice").?;
        try testing.expectEqualStrings("100", e.value);
        try testing.expectEqual(@as(u64, 100), e.lsn);
    }
    {
        const e = kv.get("counter").?;
        try testing.expectEqual(@as(usize, 8), e.value.len);
        const v = std.mem.readInt(i64, e.value[0..8], .little);
        try testing.expectEqual(@as(i64, 17), v); // 10 + 7
    }
    {
        const e = kv.get("ephemeral").?;
        try testing.expectEqualStrings("temp", e.value);
        try testing.expectEqual(future_ns, e.expiry_ns);
    }
}

fn counterBytes(v: i64) [8]u8 {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &buf, v, .little);
    return buf;
}
