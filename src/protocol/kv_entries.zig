//! The KV family's log entries. Each key is namespace-qualified
//! (`namespace\0key`; a bare key is the default namespace), so the entry
//! names its namespace without a separate field. A TTL is relative
//! milliseconds from the entry's stamp, so every replica computes the same
//! expiry from the entry alone.

const std = @import("std");
const schema = @import("schema.zig");
const entries = @import("entries.zig");
const limits = @import("limits.zig");
const kv_ops = @import("kv_ops.zig");

const Bytes = schema.Bytes;
const Optional = schema.Optional;
const Record = schema.Record;

pub const Key = Bytes(limits.MAX_QUALIFIED_KEY);
pub const Value = Bytes(kv_ops.MAX_VALUE);
pub const TtlMs = schema.DurationMs(kv_ops.MAX_TTL_MS);
pub const Delta = schema.Int(i64);

/// What a transaction's op does. A TTL of 0 on `expire` clears it.
pub const BatchKind = enum(u8) { put, delete, incr, expire };

/// One op of a committed transaction. Which of `value`, `ttl_ms` and
/// `delta` it carries follows from its kind (`batchOpRule`).
pub const BatchOp = Record(&.{
    .{ .name = "kind", .T = schema.Enum(BatchKind) },
    .{ .name = "key", .T = Key },
    .{ .name = "value", .T = Optional(Value) },
    .{ .name = "ttl_ms", .T = Optional(TtlMs) },
    .{ .name = "delta", .T = Optional(Delta) },
});

/// Everything a batch op carries but its key and value bytes: what
/// `MAX_PAYLOAD_PER_TXN`, which counts only those, leaves out.
const BATCH_OP_FRAMING = BatchOp.max_size - limits.MAX_QUALIFIED_KEY - kv_ops.MAX_VALUE;

pub const BatchOps = schema.BoundedList(BatchOp, limits.MAX_OPS_PER_TXN, limits.MAX_PAYLOAD_PER_TXN + limits.MAX_OPS_PER_TXN * BATCH_OP_FRAMING);

pub const Put = Record(&.{
    .{ .name = "key", .T = Key },
    .{ .name = "value", .T = Value },
    .{ .name = "ttl_ms", .T = Optional(TtlMs) },
});
pub const Delete = Record(&.{.{ .name = "key", .T = Key }});
pub const Incr = Record(&.{
    .{ .name = "key", .T = Key },
    .{ .name = "delta", .T = Delta },
});
pub const Expire = Record(&.{
    .{ .name = "key", .T = Key },
    .{ .name = "ttl_ms", .T = TtlMs },
});
pub const Batch = Record(&.{.{ .name = "ops", .T = BatchOps }});
/// A JSON document's change at `path`: set to `document`, or deleted when
/// there is none. Merged with the key's value at apply, so a write that
/// lands in between is not lost.
pub const JsonPatch = Record(&.{
    .{ .name = "key", .T = Key },
    .{ .name = "path", .T = schema.String(kv_ops.MAX_PATH) },
    .{ .name = "document", .T = Optional(Bytes(kv_ops.MAX_DOCUMENT)) },
});

pub const table = [_]entries.Entry{
    .{ .name = "kv.put", .type_code = 0x01, .payload = Put, .max_payload = Put.max_size, .doc = "A key's new value, with its TTL if it has one." },
    .{ .name = "kv.delete", .type_code = 0x02, .payload = Delete, .max_payload = Delete.max_size, .doc = "A key deleted." },
    .{ .name = "kv.batch", .type_code = 0x03, .payload = Batch, .max_payload = Batch.max_size, .doc = "A transaction's writes, applied as one." },
    .{ .name = "kv.incr", .type_code = 0x04, .payload = Incr, .max_payload = Incr.max_size, .doc = "A counter's delta." },
    .{ .name = "kv.expire", .type_code = 0x05, .payload = Expire, .max_payload = Expire.max_size, .doc = "A key's TTL set, or cleared with 0." },
    .{ .name = "kv.json_patch", .type_code = 0x06, .payload = JsonPatch, .max_payload = JsonPatch.max_size, .doc = "A JSON document set or deleted at a path, merged with the key's value at apply." },
};

/// The payload schema of the KV entry with UAL type `type_code`.
pub fn Payload(comptime type_code: u8) type {
    for (table) |e| if (e.type_code == type_code) return e.payload;
    @compileError(std.fmt.comptimePrint("no KV entry has type {d}", .{type_code}));
}

/// The largest KV entry: a full transaction. Nothing else in the log is
/// larger, so the ring, the apply buffer and a replication frame are sized
/// from it (`limits.MAX_LOG_ENTRY_PAYLOAD` is checked against it).
pub const MAX_PAYLOAD: usize = Batch.max_size;

/// What's wrong with a batch op, or null: each kind carries exactly the
/// fields it uses. A put's TTL is optional; a delete carries nothing more.
pub fn batchOpRule(op: BatchOp.Value) ?[]const u8 {
    const ok = switch (op.kind) {
        .put => op.value != null and op.delta == null,
        .delete => op.value == null and op.ttl_ms == null and op.delta == null,
        .incr => op.value == null and op.ttl_ms == null and op.delta != null,
        .expire => op.value == null and op.ttl_ms != null and op.delta == null,
    };
    return if (ok) null else switch (op.kind) {
        .put => "a put carries a value and no delta",
        .delete => "a delete carries only its key",
        .incr => "an incr carries only its key and delta",
        .expire => "an expire carries only its key and TTL",
    };
}

comptime {
    std.debug.assert(MAX_PAYLOAD == limits.MAX_LOG_ENTRY_PAYLOAD);
    // A single-key entry leads with its key, so waiters and metrics read
    // it without knowing which entry it is.
    for (.{ Put, Delete, Incr, Expire, JsonPatch }) |P| std.debug.assert(std.mem.eql(u8, P.field_list[0].name, "key") and P.field_list[0].T == Key);
    for (table) |e| std.debug.assert(e.max_payload <= limits.MAX_LOG_ENTRY_PAYLOAD);
}

const testing = std.testing;

test "kv entries: a batch round-trips, and each kind's fields are checked" {
    const ops = [_]BatchOp.Value{
        .{ .kind = .put, .key = "ns\x00a", .value = "1", .ttl_ms = 500, .delta = null },
        .{ .kind = .delete, .key = "b", .value = null, .ttl_ms = null, .delta = null },
        .{ .kind = .incr, .key = "c", .value = null, .ttl_ms = null, .delta = -3 },
        .{ .kind = .expire, .key = "d", .value = null, .ttl_ms = 0, .delta = null },
    };
    for (ops) |op| try testing.expectEqual(@as(?[]const u8, null), batchOpRule(op));
    var buf: [256]u8 = undefined;
    var w: schema.Writer = .{ .buf = &buf };
    try BatchOps.encodeSlice(&w, &ops);
    var diag: schema.Diagnostic = .{};
    const batch = try Batch.decodeAll(w.written(), &diag);
    var it = batch.ops.iterator();
    var i: usize = 0;
    while (try it.next()) |op| : (i += 1) {
        try testing.expectEqual(ops[i].kind, op.kind);
        try testing.expectEqualStrings(ops[i].key, op.key);
        try testing.expectEqual(ops[i].delta, op.delta);
        try testing.expectEqual(ops[i].ttl_ms, op.ttl_ms);
    }
    try testing.expectEqual(ops.len, i);

    try testing.expect(batchOpRule(.{ .kind = .put, .key = "a", .value = null, .ttl_ms = null, .delta = null }) != null);
    try testing.expect(batchOpRule(.{ .kind = .delete, .key = "a", .value = "x", .ttl_ms = null, .delta = null }) != null);
    try testing.expect(batchOpRule(.{ .kind = .incr, .key = "a", .value = null, .ttl_ms = null, .delta = null }) != null);
    try testing.expect(batchOpRule(.{ .kind = .expire, .key = "a", .value = null, .ttl_ms = null, .delta = null }) != null);
}

test "kv entries: a batch cut anywhere or with an unknown kind is refused whole" {
    const ops = [_]BatchOp.Value{
        .{ .kind = .put, .key = "a", .value = "v", .ttl_ms = null, .delta = null },
        .{ .kind = .delete, .key = "b", .value = null, .ttl_ms = null, .delta = null },
    };
    var buf: [64]u8 = undefined;
    var w: schema.Writer = .{ .buf = &buf };
    try BatchOps.encodeSlice(&w, &ops);
    const whole = w.written();
    var diag: schema.Diagnostic = .{};
    for (0..whole.len) |n| try testing.expectError(error.Refused, Batch.decodeAll(whole[0..n], &diag));
    // The second op's kind byte: after the count, and the first op's kind,
    // key, value and two absent fields.
    var bad: [64]u8 = undefined;
    @memcpy(bad[0..whole.len], whole);
    bad[2 + 1 + 2 + 1 + 1 + 4 + 1 + 1 + 1] = 9;
    try testing.expectError(error.Refused, Batch.decodeAll(bad[0..whole.len], &diag));
}
