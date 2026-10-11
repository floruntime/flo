//! kv.get and kv.set as a client runs them: a body coded by the op's
//! schema, through `Shard.runRequest`, answered on the connection. Every
//! request field has a test here, listed in `table_run.coverage`.

const std = @import("std");
const proto = @import("../protocol/proto.zig");
const schema = @import("../protocol/schema.zig");
const kv_ops = @import("../protocol/kv_ops.zig");
const router = @import("../node/router.zig");
const table_run = @import("../node/table_run.zig");
const txn_mod = @import("txn.zig");
const ns_keys = @import("../namespace/handler.zig");
const shard_mod = @import("../node/shard.zig");
const Shard = shard_mod.Shard;
const Connection = @import("../node/connection.zig").Connection;
const Partition = @import("../storage/partition.zig").Partition;

const testing = std.testing;
const get_op = table_run.op("kv.get");
const set_op = table_run.op("kv.set");
const GetBody = get_op.request.Value;
const SetBody = set_op.request.Value;

/// A shard alone, and a client on it.
const Harness = struct {
    pipe: [2]std.posix.fd_t,
    pair: [2]std.posix.fd_t,
    shard: Shard,
    conn: *Connection,
    next_id: u64 = 1,
    out: [64 * 1024]u8 = undefined,

    fn init(h: *Harness) !void {
        h.pipe = try @import("stdx").io.pipe();
        h.shard = try Shard.init(testing.allocator, 0, 1, 4096, h.pipe[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
        try testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &h.pair));
        h.conn = try h.shard.addConnection(h.pair[0]);
        h.next_id = 1;
    }

    fn deinit(h: *Harness) void {
        h.shard.deinit();
        _ = std.c.close(h.pair[1]);
        _ = std.c.close(h.pipe[0]);
        _ = std.c.close(h.pipe[1]);
    }

    /// Sends `body` as op `code` and applies what it proposed. Returns the
    /// request id.
    fn send(h: *Harness, code: u16, key: []const u8, body: []const u8) u64 {
        const id = h.next_id;
        h.next_id += 1;
        var header = proto.RequestHeader.init(@enumFromInt(code), id);
        header.payload_length = 0;
        h.conn.response_deferred = false;
        h.shard.runRequest(h.conn, .{ .header = header, .namespace = "", .key = key, .value = body }, .client);
        _ = h.shard.applyCommitted();
        return id;
    }

    /// The next answer the client has: what the shard queued for it, or
    /// what a woken waiter sent it. Null when there is none.
    fn answer(h: *Harness) !?proto.Response {
        h.shard.flushToClient(h.conn.fd);
        const hs = @sizeOf(proto.ResponseHeader);
        const n = std.c.recv(h.pair[1], &h.out, hs, std.c.MSG.DONTWAIT);
        if (n <= 0) return null;
        try testing.expectEqual(@as(isize, hs), n);
        const header = std.mem.bytesToValue(proto.ResponseHeader, h.out[0..hs]);
        var got: usize = 0;
        while (got < header.data_len) {
            const m = std.c.recv(h.pair[1], h.out[hs + got ..].ptr, header.data_len - got, 0);
            try testing.expect(m > 0);
            got += @intCast(m);
        }
        return try proto.Response.parse(h.out[0 .. hs + got]);
    }

    fn get(h: *Harness, key: []const u8, body: GetBody) !?proto.Response {
        var buf: [get_op.request.max_size]u8 = undefined;
        _ = h.send(get_op.code, key, try get_op.request.encodeInto(&buf, body));
        return h.answer();
    }

    /// Sends a set whose write wakes the waiting get `waiting`, which must
    /// answer `version` and the set's value. The waiter answers as the
    /// write applies, so before the set does.
    fn setWaking(h: *Harness, key: []const u8, body: SetBody, waiting: u64, version: u64) !void {
        var buf: [1024]u8 = undefined;
        const id = h.send(set_op.code, key, try set_op.request.encodeInto(&buf, body));
        const woken = (try h.answer()).?;
        try testing.expectEqual(waiting, woken.header.request_id);
        try expectValue(woken, version, body.value);
        const own = (try h.answer()).?;
        try testing.expectEqual(id, own.header.request_id);
        try expectSet(own, version);
    }

    fn set(h: *Harness, key: []const u8, body: SetBody) !proto.Response {
        var buf: [1024]u8 = undefined;
        _ = h.send(set_op.code, key, try set_op.request.encodeInto(&buf, body));
        return (try h.answer()).?;
    }
};

fn getBody(fields: struct { wait_ms: u64 = 0, wait_for: kv_ops.WaitFor = .exists, routing_key: ?[]const u8 = null, txn: ?u64 = null }) GetBody {
    return .{ .wait_ms = fields.wait_ms, .wait_for = fields.wait_for, .routing_key = fields.routing_key, .txn = fields.txn };
}

fn setBody(value: []const u8, fields: struct { ttl_ms: ?u64 = null, condition: kv_ops.Condition = .always, cas_version: ?u64 = null, routing_key: ?[]const u8 = null, txn: ?u64 = null }) SetBody {
    return .{ .value = value, .ttl_ms = fields.ttl_ms, .condition = fields.condition, .cas_version = fields.cas_version, .routing_key = fields.routing_key, .txn = fields.txn };
}

fn expectValue(resp: proto.Response, version: u64, value: []const u8) !void {
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
    var diag: schema.Diagnostic = .{};
    const got = try get_op.response.decodeAll(resp.data, &diag);
    try testing.expectEqual(version, got.version);
    try testing.expectEqualStrings(value, got.value);
}

fn expectSet(resp: proto.Response, version: ?u64) !void {
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
    var diag: schema.Diagnostic = .{};
    try testing.expectEqual(version, (try set_op.response.decodeAll(resp.data, &diag)).version);
}

fn expectRefused(resp: proto.Response, status: proto.StatusCode, reason: proto.Reason) !proto.Refusal {
    try testing.expectEqual(@intFromEnum(status), resp.header.status);
    const r = try resp.refused();
    try testing.expectEqual(reason, r.reason);
    try testing.expectEqual(proto.Ran.no, r.ran);
    return r;
}

fn qualified(buf: *[ns_keys.MAX_QUALIFIED_KEY]u8, key: []const u8) []const u8 {
    return ns_keys.qualifyKey(buf, "", key) catch unreachable;
}

test "kv table: a body that doesn't decode is refused unrun, naming its field and the bound it broke" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();

    // wait_ms past its cap: [present][u64], then wait_for, routing_key, txn absent.
    var long: [12]u8 = undefined;
    long[0] = 1;
    std.mem.writeInt(u64, long[1..9], kv_ops.MAX_WAIT_MS + 1, .little);
    @memcpy(long[9..12], &[_]u8{ 0, 0, 0 });
    _ = h.send(get_op.code, "k", &long);
    var r = try expectRefused((try h.answer()).?, .bad_request, .out_of_range);
    try testing.expectEqualStrings("bad request: kv.get: wait_ms is out of range (at most 300000)", r.message);

    // A byte past the end of a whole body.
    _ = h.send(get_op.code, "k", &.{ 0, 0, 0, 0, 7 });
    r = try expectRefused((try h.answer()).?, .bad_request, .malformed);
    try testing.expectEqualStrings("bad request: kv.get: 1 bytes past the end of the body", r.message);

    // Cut short inside a field, and an enum value the schema doesn't have.
    _ = h.send(get_op.code, "k", &.{ 0, 0, 0 });
    r = try expectRefused((try h.answer()).?, .bad_request, .malformed);
    try testing.expectEqualStrings("bad request: kv.get: txn is malformed", r.message);
    _ = h.send(get_op.code, "k", &.{ 0, 1, 9, 0, 0 });
    r = try expectRefused((try h.answer()).?, .bad_request, .malformed);
    try testing.expectEqualStrings("bad request: kv.get: wait_for is malformed", r.message);

    // None of them ran: nothing waits, and nothing was written.
    try testing.expectEqual(@as(u16, 0), h.shard.waiter_pool.countByKind(.kv_get));
    _ = h.send(set_op.code, "k", &.{0});
    _ = try expectRefused((try h.answer()).?, .bad_request, .malformed);
    var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    try testing.expect(h.shard.defaultPartition().kv.get(qualified(&qbuf, "k")) == null);
}

test "kv table: a set answers its version, and a get answers the value and that version" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    try expectSet(try h.set("k", setBody("one", .{})), 1);
    try expectValue((try h.get("k", getBody(.{}))).?, 1, "one");
    try expectSet(try h.set("k", setBody("two", .{})), 2);
    try expectValue((try h.get("k", getBody(.{}))).?, 2, "two");
    _ = try expectRefused((try h.get("absent", getBody(.{}))).?, .not_found, .not_found);
}

test "kv table: a key empty, holding a NUL, or in a reserved prefix is refused by name" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    _ = try expectRefused((try h.get("", getBody(.{}))).?, .bad_request, .malformed);
    // "default" keys are stored bare, so this would spell namespace b's "k".
    const nul = try expectRefused(try h.set("b\x00k", setBody("v", .{})), .bad_request, .malformed);
    try testing.expectEqualStrings("bad request: key must not contain NUL", nul.message);
    _ = try expectRefused((try h.get("b\x00k", getBody(.{}))).?, .bad_request, .malformed);
    _ = try expectRefused(try h.set("", setBody("v", .{})), .bad_request, .malformed);
    _ = try expectRefused(try h.set("_flo:x", setBody("v", .{})), .unauthorized, .reserved_key);
    _ = try expectRefused((try h.get("_flo:x", getBody(.{}))).?, .unauthorized, .reserved_key);
}

test "kv table: wait_ms with wait_for exists answers at once when the key exists, else when it is written" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    try expectSet(try h.set("here", setBody("v", .{})), 1);
    try expectValue((try h.get("here", getBody(.{ .wait_ms = 5000 }))).?, 1, "v");

    try testing.expect((try h.get("later", getBody(.{ .wait_ms = 5000 }))) == null);
    try testing.expect(h.conn.response_deferred);
    try testing.expectEqual(@as(u16, 1), h.shard.waiter_pool.countByKind(.kv_get));
    try h.setWaking("later", setBody("w", .{}), h.next_id - 1, 1);
    try testing.expectEqual(@as(u16, 0), h.shard.waiter_pool.countByKind(.kv_get));
}

test "kv table: wait_for change waits for the version after the one there" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    try expectSet(try h.set("k", setBody("v1", .{})), 1);
    try testing.expect((try h.get("k", getBody(.{ .wait_ms = 5000, .wait_for = .change }))) == null);
    try testing.expectEqual(@as(u16, 1), h.shard.waiter_pool.countByKind(.kv_get));
    try h.setWaking("k", setBody("v2", .{}), h.next_id - 1, 2);
}

test "kv table: ttl_ms is written with the value, and none means no expiry" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    try expectSet(try h.set("t", setBody("v", .{ .ttl_ms = 60_000 })), 1);
    const e = h.shard.defaultPartition().kv.get(qualified(&qbuf, "t")).?;
    try testing.expectEqual(e.timestamp_ns + 60_000 * std.time.ns_per_ms, e.expiry_ns);
    try expectSet(try h.set("n", setBody("v", .{})), 1);
    try testing.expectEqual(@as(u64, 0), h.shard.defaultPartition().kv.get(qualified(&qbuf, "n")).?.expiry_ns);
}

test "kv table: condition if_absent and if_present refuse, unrun, when they don't hold" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    _ = try expectRefused(try h.set("k", setBody("v", .{ .condition = .if_present })), .conflict, .condition_failed);
    try expectSet(try h.set("k", setBody("v", .{ .condition = .if_absent })), 1);
    _ = try expectRefused(try h.set("k", setBody("w", .{ .condition = .if_absent })), .conflict, .condition_failed);
    try expectSet(try h.set("k", setBody("w", .{ .condition = .if_present })), 2);
}

test "kv table: cas_version must be the key's version, and 0 means absent" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    _ = try expectRefused(try h.set("k", setBody("v", .{ .cas_version = 1 })), .conflict, .cas_mismatch);
    try expectSet(try h.set("k", setBody("v", .{ .cas_version = 0 })), 1);
    _ = try expectRefused(try h.set("k", setBody("w", .{ .cas_version = 0 })), .conflict, .cas_mismatch);
    try expectSet(try h.set("k", setBody("w", .{ .cas_version = 1 })), 2);
    try expectValue((try h.get("k", getBody(.{}))).?, 2, "w");
}

test "kv table: routing_key routes the request in place of its key" {
    var buf: [256]u8 = undefined;
    inline for (.{ get_op, set_op }) |op| {
        const route = table_run.routeByKey(op.name);
        const body = if (op.code == get_op.code)
            try get_op.request.encodeInto(&buf, getBody(.{ .routing_key = "user:1" }))
        else
            try set_op.request.encodeInto(&buf, setBody("v", .{ .routing_key = "user:1" }));
        const req: proto.Request = .{ .header = proto.RequestHeader.init(@enumFromInt(op.code), 1), .namespace = "ns", .key = "balance", .value = body };
        try testing.expectEqual(router.hashKeyWithNamespace("ns", "user:1"), route(req).?);
        const plain = if (op.code == get_op.code)
            try get_op.request.encodeInto(&buf, getBody(.{}))
        else
            try set_op.request.encodeInto(&buf, setBody("v", .{}));
        try testing.expectEqual(router.hashKeyWithNamespace("ns", "balance"), route(.{ .header = req.header, .namespace = "ns", .key = "balance", .value = plain }).?);
    }
}

test "kv table: a set in a transaction is buffered, read back inside it, and unseen outside until it commits" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const txn_table = &h.shard.kv_handler.txn_table;
    const id = try txn_table.begin(router.hashKeyWithNamespace("", "user:1"), router.namespaceHash(""), 0);

    try expectSet(try h.set("bal", setBody("5", .{ .routing_key = "user:1", .txn = id })), null);
    try expectValue((try h.get("bal", getBody(.{ .routing_key = "user:1", .txn = id }))).?, 0, "5");
    _ = try expectRefused((try h.get("bal", getBody(.{ .routing_key = "user:1" }))).?, .not_found, .not_found);
    // Its routing key is the transaction's, or it's refused.
    _ = try expectRefused(try h.set("bal", setBody("6", .{ .txn = id })), .bad_request, .txn_routing_key);
    _ = try expectRefused((try h.get("bal", getBody(.{ .routing_key = "user:1", .txn = id, .wait_ms = 10 }))).?, .bad_request, .conflicting_fields);
    try testing.expectEqual(@as(usize, 1), txn_table.get(id).?.ops.items.len);
}

test "kv table: a forged or stale transaction id is refused as no such transaction" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const txn_table = &h.shard.kv_handler.txn_table;
    const open = try txn_table.begin(router.hashKeyWithNamespace("", "k"), router.namespaceHash(""), 0);
    // Another shard's bits on a live counter, a counter never handed out,
    // and one dropped.
    const forged = [_]u64{ txn_mod.makeId(5, open & txn_mod.ID_COUNTER_MASK), txn_mod.makeId(0, 999), open };
    txn_table.drop(open);
    for (forged) |id| {
        _ = try expectRefused((try h.get("k", getBody(.{ .txn = id }))).?, .not_found, .no_such_transaction);
        _ = try expectRefused(try h.set("k", setBody("v", .{ .txn = id })), .not_found, .no_such_transaction);
    }
}

test "kv table: a request in a transaction goes to the leader, and to the shard its id names" {
    var buf: [64]u8 = undefined;
    const id = txn_mod.makeId(3, 7);
    const in_txn: proto.Request = .{ .header = proto.RequestHeader.init(.kv_get, 1), .namespace = "", .key = "k", .value = try get_op.request.encodeInto(&buf, getBody(.{ .txn = id })) };
    try testing.expect(table_run.writes(in_txn));
    try testing.expectEqual(@as(?u64, id), table_run.txnId(in_txn));
    var plain_buf: [64]u8 = undefined;
    const plain: proto.Request = .{ .header = in_txn.header, .namespace = "", .key = "k", .value = try get_op.request.encodeInto(&plain_buf, getBody(.{})) };
    try testing.expect(!table_run.writes(plain));
    try testing.expectEqual(@as(?u64, null), table_run.txnId(plain));
    try testing.expectEqual(@as(u16, 3), txn_mod.idShard(id));
}
