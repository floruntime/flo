//! The KV ops as a client runs them: a body coded by the op's schema,
//! through `Shard.runRequest`, answered on the connection. Every request
//! field has a test here, listed in `table_run.coverage`.

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
    /// Room for the largest answer.
    out: []u8 = &.{},

    fn init(h: *Harness) !void {
        h.pipe = try @import("stdx").io.pipe();
        h.shard = try Shard.init(testing.allocator, 0, 1, 4096, h.pipe[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{}, .{});
        try testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &h.pair));
        // As an accepted socket is: a full kernel buffer must not block the
        // shard's flush.
        try @import("stdx").net.sysFcntlSetNonblocking(h.pair[0]);
        h.conn = try h.shard.addConnection(h.pair[0]);
        h.next_id = 1;
        h.out = try testing.allocator.alloc(u8, proto.MAX_ANSWER_BYTES);
    }

    fn deinit(h: *Harness) void {
        testing.allocator.free(h.out);
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
        var header = proto.RequestHeader.init(@fromBackingInt(@intCast(code)), id);
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
        const n = std.c.recv(h.pair[1], h.out.ptr, hs, std.c.MSG.DONTWAIT);
        if (n <= 0) return null;
        try testing.expectEqual(@as(isize, hs), n);
        const header = std.mem.bytesToValue(proto.ResponseHeader, h.out[0..hs]);
        try testing.expect(hs + header.data_len <= h.out.len);
        // A large answer leaves the shard as the socket drains.
        var got: usize = 0;
        var tries: usize = 0;
        while (got < header.data_len) : (tries += 1) {
            try testing.expect(tries < 100_000);
            h.shard.flushToClient(h.conn.fd);
            const m = std.c.recv(h.pair[1], h.out[hs + got ..].ptr, header.data_len - got, std.c.MSG.DONTWAIT);
            if (m > 0) got += @intCast(m);
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

    /// Sends KV op `name` and returns its answer, which `h.out` holds until
    /// the next.
    fn call(h: *Harness, comptime name: []const u8, key: []const u8, body: table_run.op(name).request.Value) !proto.Response {
        const op = table_run.op(name);
        const buf = try testing.allocator.alloc(u8, op.request.max_size);
        defer testing.allocator.free(buf);
        _ = h.send(op.code, key, try op.request.encodeInto(buf, body));
        return (try h.answer()).?;
    }

    /// `call`, which must answer ok: its decoded answer.
    fn ok(h: *Harness, comptime name: []const u8, key: []const u8, body: table_run.op(name).request.Value) !table_run.op(name).response.Value {
        const resp = try h.call(name, key, body);
        if (resp.header.status != @backingInt(proto.StatusCode.ok)) {
            std.debug.print("{s} refused: {s}\n", .{ name, (try resp.refused()).message });
            return error.TestUnexpectedResult;
        }
        var diag: schema.Diagnostic = .{};
        return table_run.op(name).response.decodeAll(resp.data, &diag);
    }

    /// `call`, which must be refused with `status` and `reason`, unrun.
    fn no(h: *Harness, comptime name: []const u8, key: []const u8, body: table_run.op(name).request.Value, status: proto.StatusCode, reason: proto.Reason) !void {
        _ = try expectRefused(try h.call(name, key, body), status, reason);
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
    try testing.expectEqual(@backingInt(proto.StatusCode.ok), resp.header.status);
    var diag: schema.Diagnostic = .{};
    const got = try get_op.response.decodeAll(resp.data, &diag);
    try testing.expectEqual(version, got.version);
    try testing.expectEqualStrings(value, got.value);
}

fn expectSet(resp: proto.Response, version: ?u64) !void {
    try testing.expectEqual(@backingInt(proto.StatusCode.ok), resp.header.status);
    var diag: schema.Diagnostic = .{};
    try testing.expectEqual(version, (try set_op.response.decodeAll(resp.data, &diag)).version);
}

fn expectRefused(resp: proto.Response, status: proto.StatusCode, reason: proto.Reason) !proto.Refusal {
    try testing.expectEqual(@backingInt(status), resp.header.status);
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

test "kv table: every op that takes a key refuses one in a reserved prefix" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    var checked: usize = 0;
    inline for (kv_ops.table) |op| {
        // scan's key is a prefix and mget's keys are in its body (both
        // tested on their own); a transaction's begin key is its routing
        // key, and commit and rollback take none.
        const skip = comptime for ([_][]const u8{ "kv.scan", "kv.mget", "kv.txn.begin", "kv.txn.commit", "kv.txn.rollback" }) |name| {
            if (std.mem.eql(u8, op.name, name)) break true;
        } else false;
        if (!skip) {
            var body = plainBody(op);
            if (@hasField(op.request.Value, "document")) body.document = "{}";
            try h.no(op.name, "_flo:x", body, .unauthorized, .reserved_key);
            checked += 1;
        }
    }
    try testing.expectEqual(@as(usize, 10), checked);
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

test "kv table: ttl_ms is written with the value, get answers when it runs out, and none means no expiry" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    try expectSet(try h.set("t", setBody("v", .{ .ttl_ms = 60_000 })), 1);
    const e = h.shard.defaultPartition().kv.get(qualified(&qbuf, "t")).?;
    try testing.expectEqual(e.timestamp_ns + 60_000 * std.time.ns_per_ms, e.expiry_ns);
    try testing.expectEqual(@as(?i64, @intCast(e.expiry_ns / std.time.ns_per_ms)), (try h.ok("kv.get", "t", getBody(.{}))).expires_at_ms);
    try expectSet(try h.set("n", setBody("v", .{})), 1);
    try testing.expectEqual(@as(u64, 0), h.shard.defaultPartition().kv.get(qualified(&qbuf, "n")).?.expiry_ns);
    try testing.expectEqual(@as(?i64, null), (try h.ok("kv.get", "n", getBody(.{}))).expires_at_ms);
}

test "kv table: a waiting get answers the key's expiry too" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    try testing.expect((try h.get("t", getBody(.{ .wait_ms = 5000 }))) == null);
    var buf: [1024]u8 = undefined;
    _ = h.send(set_op.code, "t", try set_op.request.encodeInto(&buf, setBody("v", .{ .ttl_ms = 60_000 })));
    const woken = (try h.answer()).?;
    var diag: schema.Diagnostic = .{};
    const got = try get_op.response.decodeAll(woken.data, &diag);
    const e = h.shard.defaultPartition().kv.get(qualified(&qbuf, "t")).?;
    try testing.expectEqual(@as(?i64, @intCast(e.expiry_ns / std.time.ns_per_ms)), got.expires_at_ms);
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
    var buf: [512]u8 = undefined;
    var routed: usize = 0;
    inline for (kv_ops.table) |op| {
        // mget routes on its routing_key field alone: tested on its own.
        if (@hasField(op.request.Value, "routing_key") and op.route == .key) {
            routed += 1;
            const route = table_run.routeByKey(op.name);
            const header = proto.RequestHeader.init(@fromBackingInt(@intCast(op.code)), 1);
            var given = plainBody(op);
            given.routing_key = "user:1";
            const req: proto.Request = .{ .header = header, .namespace = "ns", .key = "balance", .value = try op.request.encodeInto(&buf, given) };
            try testing.expectEqual(router.hashKeyWithNamespace("ns", "user:1"), route(req).?);
            const plain: proto.Request = .{ .header = header, .namespace = "ns", .key = "balance", .value = try op.request.encodeInto(&buf, plainBody(op)) };
            try testing.expectEqual(router.hashKeyWithNamespace("ns", "balance"), route(plain).?);
        }
    }
    try testing.expectEqual(@as(usize, 10), routed);
}

test "kv table: kv.mget routes on its routing key, and stays where it lands without one" {
    const op = table_run.op("kv.mget");
    const route = table_run.routeByField("kv.mget");
    var kbuf: [16]u8 = undefined;
    var keys = op.request.field_list[0].T.Builder.init(&kbuf);
    try testing.expect(try keys.add("balance"));
    var buf: [64]u8 = undefined;
    const header = proto.RequestHeader.init(.kv_mget, 1);
    const routed: proto.Request = .{ .header = header, .namespace = "ns", .key = "", .value = try op.request.encodeInto(&buf, .{ .keys = keys.value(), .routing_key = "user:1" }) };
    try testing.expectEqual(router.hashKeyWithNamespace("ns", "user:1"), route(routed).?);
    var buf2: [64]u8 = undefined;
    const plain: proto.Request = .{ .header = header, .namespace = "ns", .key = "", .value = try op.request.encodeInto(&buf2, .{ .keys = keys.value(), .routing_key = null }) };
    try testing.expectEqual(@as(?u64, null), route(plain));
}

/// A body for `op` with every field absent, at its default, or empty.
fn plainBody(comptime op: anytype) op.request.Value {
    var v: op.request.Value = undefined;
    inline for (op.request.field_list) |f| {
        @field(v, f.name) = switch (f.T.kind) {
            .optional => null,
            .defaulted => f.T.default_value,
            .bytes, .string, .parsed => "",
            .integer => 0,
            else => @compileError("no plain value for field " ++ f.name),
        };
    }
    return v;
}

test "kv table: a set in a transaction is buffered, read back inside it, and unseen outside until it commits" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const txn_table = &h.shard.kv_handler.txn_table;
    const id = try txn_table.begin(router.hashKeyWithNamespace("", "user:1"), router.namespaceHash(""));

    try expectSet(try h.set("bal", setBody("5", .{ .routing_key = "user:1", .txn = id })), null);
    try expectValue((try h.get("bal", getBody(.{ .routing_key = "user:1", .txn = id }))).?, 0, "5");
    _ = try expectRefused((try h.get("bal", getBody(.{ .routing_key = "user:1" }))).?, .not_found, .not_found);
    // Its routing key is the transaction's, or it's refused.
    _ = try expectRefused(try h.set("bal", setBody("6", .{ .txn = id })), .bad_request, .wrong_routing_key);
    _ = try expectRefused((try h.get("bal", getBody(.{ .routing_key = "user:1", .txn = id, .wait_ms = 10 }))).?, .bad_request, .conflicting_fields);
    try testing.expectEqual(@as(usize, 1), txn_table.get(id).?.ops.items.len);
}

test "kv table: a forged or stale transaction id is refused as no such transaction" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const txn_table = &h.shard.kv_handler.txn_table;
    const open = try txn_table.begin(router.hashKeyWithNamespace("", "k"), router.namespaceHash(""));
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

test "kv table: the longest value applies with the longest key and routing key, TTL and cas, and one byte more is refused unrun" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const a = testing.allocator;
    const key = try a.alloc(u8, 4000);
    defer a.free(key);
    @memset(key, 'k');
    const rk = try a.alloc(u8, 4000);
    defer a.free(rk);
    @memset(rk, 'r');

    // Hand-coded so the over-long value reaches the server.
    const body = try a.alloc(u8, 4 + kv_ops.MAX_VALUE + 1 + 9 + 2 + 9 + 3 + rk.len + 1);
    defer a.free(body);
    for ([_]u32{ kv_ops.MAX_VALUE, kv_ops.MAX_VALUE + 1 }) |n| {
        var w: schema.Writer = .{ .buf = body };
        try w.int(u32, n);
        @memset(body[4..][0..n], 'v');
        w.pos += n;
        try w.put(&.{1});
        try w.int(u64, 60_000); // ttl_ms
        try w.put(&.{ 1, 0 }); // condition always
        try w.put(&.{1});
        try w.int(u64, 0); // cas_version: absent
        try w.put(&.{1});
        try w.int(u16, @intCast(rk.len));
        try w.put(rk);
        try w.put(&.{0}); // no txn
        _ = h.send(set_op.code, key, w.written());
        const resp = (try h.answer()).?;
        if (n == kv_ops.MAX_VALUE) {
            try expectSet(resp, 1);
        } else {
            const r = try expectRefused(resp, .bad_request, .field_too_long);
            try testing.expectEqualStrings("bad request: kv.set: value is longer than 253817 bytes", r.message);
        }
    }
    var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    try testing.expectEqual(@as(usize, kv_ops.MAX_VALUE), h.shard.defaultPartition().kv.get(qualified(&qbuf, key)).?.value.len);
}

test "kv table: a get waits on a key past the waiter's inline buffer" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const key = @as([300]u8, @splat('k'));
    try testing.expect((try h.get(&key, getBody(.{ .wait_ms = 5000 }))) == null);
    try testing.expectEqual(@as(u16, 1), h.shard.waiter_pool.countByKind(.kv_get));
    try h.setWaking(&key, setBody("w", .{}), h.next_id - 1, 1);
}

test "kv ops: delete answers whether the key existed, and cas_version guards it" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    try testing.expectEqual(@as(?bool, false), (try h.ok("kv.delete", "k", .{ .cas_version = null, .routing_key = null, .txn = null })).existed);
    try expectSet(try h.set("k", setBody("v", .{})), 1);
    try h.no("kv.delete", "k", .{ .cas_version = 7, .routing_key = null, .txn = null }, .conflict, .cas_mismatch);
    try testing.expectEqual(@as(?bool, true), (try h.ok("kv.delete", "k", .{ .cas_version = 1, .routing_key = null, .txn = null })).existed);
    _ = try expectRefused((try h.get("k", getBody(.{}))).?, .not_found, .not_found);
    // A key the server keeps is refused, not reported absent.
    try h.no("kv.delete", "_flo:x", .{ .cas_version = null, .routing_key = null, .txn = null }, .unauthorized, .reserved_key);
}

test "kv ops: delete, incr, expire and exists in a transaction are buffered, and commit writes them as one" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    for ([_][]const u8{ "gone", "ttl" }) |k| try expectSet(try h.set(k, setBody("v", .{ .routing_key = "rk" })), 1);
    const begun = try h.ok("kv.txn.begin", "rk", .{});
    try testing.expectEqual(router.hashKeyWithNamespace("", "rk"), begun.pinned);
    const txn = begun.txn;

    try testing.expectEqual(@as(?bool, null), (try h.ok("kv.delete", "gone", .{ .cas_version = null, .routing_key = "rk", .txn = txn })).existed);
    const counted = try h.ok("kv.incr", "count", .{ .delta = 5, .routing_key = "rk", .txn = txn });
    try testing.expectEqual(@as(?u64, null), counted.version);
    try testing.expectEqual(@as(?i64, null), counted.value);
    _ = try h.ok("kv.expire", "ttl", .{ .ttl_ms = 60_000, .cas_version = null, .routing_key = "rk", .txn = txn });
    // Inside, the delete shows; outside, nothing has happened yet.
    try testing.expect(!(try h.ok("kv.exists", "gone", .{ .routing_key = "rk", .txn = txn })).exists);
    try testing.expect((try h.ok("kv.exists", "gone", .{ .routing_key = "rk", .txn = null })).exists);
    try h.no("kv.incr", "count", .{ .delta = 1, .routing_key = "other", .txn = txn }, .bad_request, .wrong_routing_key);

    try testing.expectEqual(@as(u16, 3), (try h.ok("kv.txn.commit", "", .{ .txn = txn })).ops);
    var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    const kv = &h.shard.defaultPartition().kv;
    try testing.expect(kv.get(qualified(&qbuf, "gone")) == null);
    try testing.expectEqual(@as(i64, 5), std.mem.readInt(i64, kv.get(qualified(&qbuf, "count")).?.value[0..8], .little));
    try testing.expect(kv.get(qualified(&qbuf, "ttl")).?.expiry_ns != 0);
    // Committed: the id names no transaction now.
    try h.no("kv.txn.commit", "", .{ .txn = txn }, .not_found, .no_such_transaction);
}

test "kv ops: rollback drops a transaction's writes, and then neither rollback nor commit finds it" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const txn = (try h.ok("kv.txn.begin", "rk", .{})).txn;
    try expectSet(try h.set("k", setBody("v", .{ .routing_key = "rk", .txn = txn })), null);
    _ = try h.ok("kv.txn.rollback", "", .{ .txn = txn });
    _ = try expectRefused((try h.get("k", getBody(.{}))).?, .not_found, .not_found);
    try h.no("kv.txn.rollback", "", .{ .txn = txn }, .not_found, .no_such_transaction);
    try h.no("kv.txn.commit", "", .{ .txn = txn }, .not_found, .no_such_transaction);
    // Begun with no key, a transaction has nothing to pin it.
    try h.no("kv.txn.begin", "", .{}, .bad_request, .malformed);
}

test "kv ops: scan pages names by limit and cursor, under a prefix, without reserved keys" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    for ([_][]const u8{ "a1", "a2", "a3", "a4", "a5", "b1" }) |k| try expectSet(try h.set(k, setBody("v", .{})), 1);
    // Reserved keys only come from the server itself.
    var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    try h.shard.defaultPartition().kv.put(qualified(&qbuf, "_flo:x"), "v", 1, 0, 0, 0);

    var seen: [5][2]u8 = undefined;
    var n: usize = 0;
    var cursor_buf: [kv_ops.MAX_CURSOR]u8 = undefined;
    var cursor: []const u8 = "";
    var pages: usize = 0;
    while (true) : (pages += 1) {
        const page = try h.ok("kv.scan", "a", .{ .limit = 2, .cursor = cursor });
        try testing.expect(page.names.count <= 2);
        var it = page.names.iterator();
        while (try it.next()) |name| {
            @memcpy(&seen[n], name[0..2]);
            n += 1;
        }
        const next = page.cursor orelse break;
        @memcpy(cursor_buf[0..next.len], next);
        cursor = cursor_buf[0..next.len];
    }
    try testing.expectEqual(@as(usize, 5), n);
    try testing.expect(pages >= 2);
    for (seen) |name| try testing.expectEqual(@as(u8, 'a'), name[0]);
    const all = try h.ok("kv.scan", "", .{ .limit = kv_ops.MAX_SCAN, .cursor = "" });
    try testing.expectEqual(@as(u32, 6), all.names.count);
}

test "kv ops: scan pages long names without dropping any, though they overflow an answer" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const a = testing.allocator;
    // 80 names of 4000 bytes: more than one answer holds, fewer than the limit.
    const key = try a.alloc(u8, 4000);
    defer a.free(key);
    @memset(key, 'k');
    for (0..80) |i| {
        _ = try std.fmt.bufPrint(key[0..2], "{d:0>2}", .{i});
        try expectSet(try h.set(key, setBody("v", .{})), 1);
    }

    var seen = @as([80]bool, @splat(false));
    var cursor_buf: [kv_ops.MAX_CURSOR]u8 = undefined;
    var cursor: []const u8 = "";
    var pages: usize = 0;
    while (true) : (pages += 1) {
        const page = try h.ok("kv.scan", "", .{ .limit = kv_ops.MAX_SCAN, .cursor = cursor });
        var it = page.names.iterator();
        while (try it.next()) |name| {
            const i = try std.fmt.parseInt(usize, name[0..2], 10);
            try testing.expect(!seen[i]);
            seen[i] = true;
        }
        const next = page.cursor orelse break;
        @memcpy(cursor_buf[0..next.len], next);
        cursor = cursor_buf[0..next.len];
    }
    for (seen) |s| try testing.expect(s);
    try testing.expect(pages >= 1);
}

test "kv ops: history answers versions newest first, at most limit; a missing key is not found" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    for ([_][]const u8{ "v1", "v2", "v3" }) |v| _ = try h.set("k", setBody(v, .{}));
    const two = try h.ok("kv.history", "k", .{ .limit = 2, .routing_key = null });
    try testing.expectEqual(@as(u32, 2), two.versions.count);
    var it = two.versions.iterator();
    for ([_]struct { version: u64, value: []const u8 }{ .{ .version = 3, .value = "v3" }, .{ .version = 2, .value = "v2" } }) |want| {
        const v = (try it.next()).?;
        try testing.expectEqual(want.version, v.version);
        try testing.expectEqualStrings(want.value, v.value);
        try testing.expect(v.at_ms > 0);
    }
    try testing.expectEqual(@as(u32, 3), (try h.ok("kv.history", "k", .{ .limit = 10, .routing_key = null })).versions.count);
    try h.no("kv.history", "absent", .{ .limit = 10, .routing_key = null }, .not_found, .not_found);
}

test "kv ops: history marks a delete's version deleted" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    _ = try h.set("k", setBody("v1", .{}));
    _ = try h.ok("kv.delete", "k", .{ .cas_version = null, .routing_key = null, .txn = null });
    _ = try h.set("k", setBody("v3", .{}));
    var it = (try h.ok("kv.history", "k", .{ .limit = 10, .routing_key = null })).versions.iterator();
    for ([_]struct { version: u64, deleted: bool }{ .{ .version = 3, .deleted = false }, .{ .version = 2, .deleted = true }, .{ .version = 1, .deleted = false } }) |want| {
        const v = (try it.next()).?;
        try testing.expectEqual(want.version, v.version);
        try testing.expectEqual(want.deleted, v.deleted);
    }
    try testing.expect((try it.next()) == null);
}

test "kv ops: mget answers each key in order, found or not, and next when they don't all fit" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const a = testing.allocator;
    const big = try a.alloc(u8, 100 * 1024);
    defer a.free(big);
    @memset(big, 'x');
    for ([_][]const u8{ "b1", "b2", "b3" }) |k| {
        const body = try a.alloc(u8, set_op.request.max_size);
        defer a.free(body);
        _ = h.send(set_op.code, k, try kv_ops.plainSetBody(body, big));
        try expectSet((try h.answer()).?, 1);
    }
    try expectSet(try h.set("small", setBody("s", .{})), 1);

    const Keys = table_run.op("kv.mget").request.field_list[0].T;
    var kbuf: [256]u8 = undefined;
    var keys = Keys.Builder.init(&kbuf);
    for ([_][]const u8{ "small", "absent", "_flo:x", "b1", "b2", "b3" }) |k| try testing.expect(try keys.add(k));
    const got = try h.ok("kv.mget", "", .{ .keys = keys.value(), .routing_key = null });
    // Two 100 KiB values fill the answer; the third goes in the next ask.
    try testing.expectEqual(@as(?u16, 5), got.next);
    try testing.expectEqual(@as(u32, 5), got.entries.count);
    var it = got.entries.iterator();
    const small = (try it.next()).?;
    try testing.expect(small.found);
    try testing.expectEqualStrings("s", small.value);
    try testing.expect(!(try it.next()).?.found);
    try testing.expect(!(try it.next()).?.found); // a reserved key is not found
    try testing.expectEqual(@as(usize, big.len), (try it.next()).?.value.len);

    var rest = Keys.Builder.init(&kbuf);
    try testing.expect(try rest.add("b3"));
    const last = try h.ok("kv.mget", "", .{ .keys = rest.value(), .routing_key = null });
    try testing.expectEqual(@as(?u16, null), last.next);
    try testing.expectEqual(@as(u32, 1), last.entries.count);
}

test "kv ops: incr adds delta to a counter from 0, and refuses a value that isn't one or a sum past i64" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const once = try h.ok("kv.incr", "c", .{ .delta = 1, .routing_key = null, .txn = null });
    try testing.expectEqual(@as(?i64, 1), once.value);
    try testing.expectEqual(@as(?u64, 1), once.version);
    try testing.expectEqual(@as(?i64, -4), (try h.ok("kv.incr", "c", .{ .delta = -5, .routing_key = null, .txn = null })).value);
    try expectSet(try h.set("text", setBody("hello", .{})), 1);
    try h.no("kv.incr", "text", .{ .delta = 1, .routing_key = null, .txn = null }, .bad_request, .wrong_type);
    _ = try h.ok("kv.incr", "max", .{ .delta = std.math.maxInt(i64), .routing_key = null, .txn = null });
    try h.no("kv.incr", "max", .{ .delta = 1, .routing_key = null, .txn = null }, .bad_request, .out_of_range);
}

test "kv ops: expire sets a TTL from now, 0 clears it, and cas_version guards it; a missing key is not found" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    const kv = &h.shard.defaultPartition().kv;
    try expectSet(try h.set("k", setBody("v", .{})), 1);
    _ = try h.ok("kv.expire", "k", .{ .ttl_ms = 60_000, .cas_version = null, .routing_key = null, .txn = null });
    const e = kv.get(qualified(&qbuf, "k")).?;
    try testing.expectEqual(e.timestamp_ns + 60_000 * std.time.ns_per_ms, e.expiry_ns);
    try h.no("kv.expire", "k", .{ .ttl_ms = 0, .cas_version = 99, .routing_key = null, .txn = null }, .conflict, .cas_mismatch);
    _ = try h.ok("kv.expire", "k", .{ .ttl_ms = 0, .cas_version = kv.get(qualified(&qbuf, "k")).?.version, .routing_key = null, .txn = null });
    try testing.expectEqual(@as(u64, 0), kv.get(qualified(&qbuf, "k")).?.expiry_ns);
    try h.no("kv.expire", "absent", .{ .ttl_ms = 1000, .cas_version = null, .routing_key = null, .txn = null }, .not_found, .not_found);
}

test "kv ops: exists answers whether a key exists, and its version" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const none = try h.ok("kv.exists", "k", .{ .routing_key = null, .txn = null });
    try testing.expect(!none.exists);
    try expectSet(try h.set("k", setBody("v", .{})), 1);
    try expectSet(try h.set("k", setBody("w", .{})), 2);
    const some = try h.ok("kv.exists", "k", .{ .routing_key = null, .txn = null });
    try testing.expect(some.exists);
    try testing.expectEqual(@as(u64, 2), some.version);
}

test "kv ops: json_get, json_set and json_del work at a path, and $ is the whole document" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    // $ on a missing key writes the whole document; another path is not found.
    try h.no("kv.json.set", "doc", .{ .path = "$.a", .document = "1", .routing_key = null }, .not_found, .not_found);
    try testing.expectEqual(@as(u64, 1), (try h.ok("kv.json.set", "doc", .{ .path = "$", .document = "{\"a\":1,\"b\":{\"c\":2}}", .routing_key = null })).version);
    try testing.expectEqual(@as(u64, 2), (try h.ok("kv.json.set", "doc", .{ .path = "$.a", .document = "5", .routing_key = null })).version);
    const a = try h.ok("kv.json.get", "doc", .{ .path = "$.a", .routing_key = null });
    try testing.expectEqualStrings("5", a.document);
    try testing.expectEqual(@as(u64, 2), a.version);
    try testing.expectEqual(@as(?u64, 3), (try h.ok("kv.json.del", "doc", .{ .path = "$.b", .routing_key = null })).version);
    try h.no("kv.json.get", "doc", .{ .path = "$.b", .routing_key = null }, .not_found, .not_found);

    try h.no("kv.json.set", "doc", .{ .path = "$", .document = "{not json", .routing_key = null }, .bad_request, .malformed);
    try h.no("kv.json.get", "doc", .{ .path = "a.b[", .routing_key = null }, .bad_request, .malformed);
    try expectSet(try h.set("text", setBody("hello", .{})), 1);
    try h.no("kv.json.get", "text", .{ .path = "$.a", .routing_key = null }, .bad_request, .wrong_type);

    // $ deletes the key.
    try testing.expectEqual(@as(?u64, null), (try h.ok("kv.json.del", "doc", .{ .path = "$", .routing_key = null })).version);
    try h.no("kv.json.get", "doc", .{ .path = "$", .routing_key = null }, .not_found, .not_found);
}
