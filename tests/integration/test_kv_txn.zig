//! Integration: per-shard KV transactions (batched commit path).
//!
//! Covers the in-memory side of the BEGIN→stage-ops→COMMIT cycle:
//!   1. TxnTable: open, append several mixed ops, drop.
//!   2. Codec: a buffered transaction encodes to the batch entry and decodes back.
//!   3. Projection: apply a synthetic kv_batch UAL entry and verify state.

const std = @import("std");
const testing = std.testing;
const src = @import("src");

const txn_mod = src.kv.txn;
const KVProjection = src.projection.kv.KVProjection;
const ual_entry = src.storage.ual.entry;
const kv_entries = src.protocol.kv_entries;
const schema = src.protocol.schema;

test "integration: kv_txn — table appends ops and drops cleanly" {
    var table = txn_mod.TxnTable.init(testing.allocator);
    defer table.deinit();

    const tid = try table.begin(0xDEAD_BEEF, 42);
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

    // Last-op-for-key lookup powers RYW reads.
    try testing.expect(table.lastOpForKey(tid, "alice") != null);
    try testing.expectEqual(txn_mod.TxnOpKind.put, table.lastOpForKey(tid, "alice").?.kind);
    try testing.expectEqual(txn_mod.TxnOpKind.delete, table.lastOpForKey(tid, "bob").?.kind);
    try testing.expect(table.lastOpForKey(tid, "missing") == null);

    table.drop(tid);
    try testing.expect(table.get(tid) == null);
}

test "integration: kv_txn — batch payload round-trips through codec" {
    var table = txn_mod.TxnTable.init(testing.allocator);
    defer table.deinit();

    const tid = try table.begin(0, 99);
    try table.appendOp(tid, .put, "k1", "v1", 1_000);
    try table.appendOp(tid, .put, "k2", "v2", 0);
    try table.appendOp(tid, .delete, "k3", "", 0);

    const state = table.get(tid).?;
    const payload = try txn_mod.encodeBatch(testing.allocator, state.ops.items);
    defer testing.allocator.free(payload);
    var diag: schema.Diagnostic = .{};
    const batch = try kv_entries.Batch.decodeAll(payload, &diag);

    var it = batch.ops.iterator();
    var seen: usize = 0;
    while (try it.next()) |op| : (seen += 1) {
        switch (seen) {
            0 => {
                try testing.expectEqual(txn_mod.TxnOpKind.put, op.kind);
                try testing.expectEqualStrings("k1", op.key);
                try testing.expectEqualStrings("v1", op.value.?);
                try testing.expectEqual(@as(?u64, 1_000), op.ttl_ms);
            },
            1 => {
                try testing.expectEqual(txn_mod.TxnOpKind.put, op.kind);
                try testing.expectEqualStrings("k2", op.key);
                try testing.expectEqual(@as(?u64, null), op.ttl_ms);
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

    ops[0] = .{ .kind = .put, .key = @constCast("alice"), .value = @constCast("100"), .ttl_ms = 0 };
    ops[1] = .{ .kind = .delete, .key = @constCast("doomed"), .value = @constCast(""), .ttl_ms = 0 };
    ops[2] = .{ .kind = .incr, .key = @constCast("counter"), .value = &delta_value, .ttl_ms = 0 };
    // A minute's TTL, counted from the batch's stamp.
    ops[3] = .{ .kind = .put, .key = @constCast("ephemeral"), .value = @constCast("temp"), .ttl_ms = 60_000 };

    const payload = try txn_mod.encodeBatch(testing.allocator, &ops);
    defer testing.allocator.free(payload);

    const entry = ual_entry.buildEntry(.kv_batch, 0, 1, 100, 12345, payload);
    var result = src.projection.kv.Result.start(&entry);
    try kv.applyEntry(&entry, &result);
    // The batch answers with its op count.
    var r = result.reader();
    try testing.expectEqual(@as(?u16, 4), r.int(u16));

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
        // Raw: the stamp is long past by the wall clock a read judges by.
        const e = kv.getRaw("ephemeral").?;
        try testing.expectEqualStrings("temp", e.value);
        try testing.expectEqual(@as(u64, 12345 + 60 * std.time.ns_per_s), e.expiry_ns);
    }
}

fn counterBytes(v: i64) [8]u8 {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &buf, v, .little);
    return buf;
}
