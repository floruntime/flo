//! Runs the ops the op table declares. A family binds a function to each
//! of its table ops; `handler` decodes the request body by the op's schema,
//! refuses a body that doesn't decode, naming the field and the bound it
//! broke, and calls the function with each field as its own argument. The
//! function answers with an `Outcome`, or with an error that its family's
//! `refusal` turns into a reason. An error with no mapping fails the build,
//! and so does a function whose parameters aren't the op's fields.

const std = @import("std");
const proto = @import("../protocol/proto.zig");
const ops = @import("../protocol/ops.zig");
const schema = @import("../protocol/schema.zig");
const table = @import("../protocol/table.zig");
const router = @import("router.zig");
const dispatcher_mod = @import("dispatcher.zig");
const ProposeResult = @import("../raft/types.zig").ProposeResult;
const Shard = @import("shard.zig").Shard;
const Connection = @import("connection.zig").Connection;

/// What a table op's function did.
pub fn Outcome(comptime Response: type) type {
    return union(enum) {
        /// Answer now, with this.
        answer: Response.Value,
        /// The write was proposed: `answer` answers from its result once it
        /// applies.
        park: Park,
        /// A waiter holds the request, and answers it.
        wait,
        /// Refuse it, with a message the family's errors don't fix.
        refused: Refused,
    };
}

/// `release`, given `ctx`, frees what the write holds however its wait ends.
pub const Park = struct { proposed: ProposeResult, answer: Shard.AnswerFn, release: ?Shard.ReleaseFn = null, ctx: u64 = 0 };

/// A refusal and the status it goes with: what a family's `refusal` gives.
pub const Refused = struct { status: proto.StatusCode, refusal: proto.Refusal };

/// The op in the table named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table.op_table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no table op named '" ++ name ++ "'");
}

/// The dispatcher's handler for op `name`: decodes, then calls `function`
/// as `function(shard, conn, req, field...)`. `Family` declares `Error` and
/// `refusal(Error) Refused`.
pub fn handler(comptime name: []const u8, comptime Family: type, comptime function: anytype) dispatcher_mod.HandlerFn {
    const the_op = op(name);
    const Request = the_op.request;
    const fields = Request.field_list;
    const params = @typeInfo(@TypeOf(function)).@"fn".param_types;
    if (params.len != 3 + fields.len)
        @compileError(std.fmt.comptimePrint("{s}: its function takes {d} fields, the op declares {d}", .{ name, params.len - 3, fields.len }));
    inline for (fields, 0..) |f, i| {
        if (params[3 + i] != f.T.Value) @compileError(name ++ ": parameter " ++ std.fmt.comptimePrint("{d}", .{i + 3}) ++ " isn't field '" ++ f.name ++ "'s type");
    }
    return struct {
        fn run(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: proto.Request) void {
            const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
            const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
            var diag: schema.Diagnostic = .{};
            const body = Request.decodeAll(req.value, &diag) catch {
                var buf: [256]u8 = undefined;
                return shard.sendRefusal(conn, req.header.request_id, .bad_request, .{ .reason = diag.reason, .ran = .no, .message = decodeMessage(&buf, name, diag) });
            };
            var args: std.meta.ArgsTuple(@TypeOf(function)) = undefined;
            args[0] = shard;
            args[1] = conn;
            args[2] = req;
            inline for (fields, 0..) |f, i| args[3 + i] = @field(body, f.name);
            const outcome = @call(.auto, function, args) catch |err| {
                const r = Family.refusal(err);
                return shard.sendRefusal(conn, req.header.request_id, r.status, r.refusal);
            };
            switch (outcome) {
                .answer => |v| answer(the_op.response, shard, conn, req.header.request_id, v),
                .park => |p| shard.parkHolding(conn, req, p.proposed, p.answer, p.release, p.ctx),
                .wait => conn.response_deferred = true,
                .refused => |r| shard.sendRefusal(conn, req.header.request_id, r.status, r.refusal),
            }
        }
    }.run;
}

/// Answers `request_id` with `v`, encoded by `Response` straight into the
/// connection's unsent bytes: sized first, so a small answer takes only its
/// own bytes, whatever the op's largest.
pub fn answer(comptime Response: type, shard: *Shard, conn: *Connection, request_id: u64, v: Response.Value) void {
    const header_size = @sizeOf(proto.ResponseHeader);
    const body_size = Response.encodedSize(v) catch
        return shard.sendErrorResponse(conn, request_id, .internal_error, "internal error: an answer didn't fit its schema");
    if (body_size > Response.max_size) return shard.sendErrorResponse(conn, request_id, .internal_error, "internal error: an answer didn't fit its schema");
    // Null only where `queueWrite` would refuse it too: the client isn't
    // reading, and is closed.
    const out = conn.reserveWrite(header_size + body_size) orelse {
        conn.write_overflow = true;
        return;
    };
    const body = Response.encodeInto(out[header_size..], v) catch unreachable; // sized above
    conn.commitWrite(proto.Response.sealOk(request_id, out, body.len).len);
}

/// `v` as a whole ok answer in `buf`, or null when it breaks `Response`'s
/// bounds: a server bug, never the request's.
pub fn encodeAnswer(comptime Response: type, buf: *[@sizeOf(proto.ResponseHeader) + Response.max_size]u8, request_id: u64, v: Response.Value) ?[]const u8 {
    const body = Response.encodeInto(buf[@sizeOf(proto.ResponseHeader)..], v) catch return null;
    return proto.Response.sealOk(request_id, buf, body.len);
}

/// A decode refusal's message: the op, the field and the bound it broke.
fn decodeMessage(buf: []u8, comptime name: []const u8, diag: schema.Diagnostic) []const u8 {
    const field = if (diag.field.len == 0) "body" else diag.field;
    return switch (diag.reason) {
        .field_too_long => std.fmt.bufPrint(buf, "bad request: {s}: {s} is longer than {d} bytes", .{ name, field, diag.bound }),
        .out_of_range => std.fmt.bufPrint(buf, "bad request: {s}: {s} is out of range (at most {d})", .{ name, field, diag.bound }),
        else => if (diag.field.len == 0)
            std.fmt.bufPrint(buf, "bad request: {s}: {d} bytes past the end of the body", .{ name, diag.bound })
        else
            std.fmt.bufPrint(buf, "bad request: {s}: {s} is malformed", .{ name, field }),
    } catch "bad request: the body doesn't decode";
}

// ── What routing and admission read from a table op's body ─────────────
// Each decodes the body; one that doesn't decode is answered as the
// handler refuses it, so these treat it as carrying nothing.

/// The routing hash of a table op routed by key: its `routing_key` when
/// given, else its key.
pub fn routeByKey(comptime name: []const u8) dispatcher_mod.PreRouteFn {
    const Request = op(name).request;
    return struct {
        fn route(req: proto.Request) ?u64 {
            if (req.key.len == 0) return 0;
            if (@hasField(Request.Value, "routing_key")) {
                var diag: schema.Diagnostic = .{};
                if (Request.decodeAll(req.value, &diag)) |body| {
                    if (body.routing_key) |k| return router.hashKeyWithNamespace(req.namespace, k);
                } else |_| {}
            }
            return router.hashKeyWithNamespace(req.namespace, req.key);
        }
    }.route;
}

/// The routing hash of a table op routed by `field`: the field's hash when
/// the body carries it, else null, which leaves the request where it is.
pub fn routeByField(comptime name: []const u8) dispatcher_mod.PreRouteFn {
    const o = op(name);
    const field = o.route.field;
    return struct {
        fn route(req: proto.Request) ?u64 {
            var diag: schema.Diagnostic = .{};
            const body = o.request.decodeAll(req.value, &diag) catch return null;
            const v = @field(body, field) orelse return null;
            return router.hashKeyWithNamespace(req.namespace, v);
        }
    }.route;
}

/// Whether a table op's body decodes.
pub fn decodes(req: proto.Request) bool {
    inline for (table.op_table) |o| {
        if (o.code == req.header.op_code) {
            var diag: schema.Diagnostic = .{};
            _ = o.request.decodeAll(req.value, &diag) catch return false;
            return true;
        }
    }
    return false;
}

/// How long a table op asks to wait, or null for no wait.
pub fn waitMs(req: proto.Request) ?u64 {
    inline for (table.op_table) |o| {
        if (o.code == req.header.op_code) {
            const w = o.wait orelse return null;
            var diag: schema.Diagnostic = .{};
            const body = o.request.decodeAll(req.value, &diag) catch return null;
            const ms: u64 = @field(body, w.field);
            return if (ms == 0) null else ms;
        }
    }
    return null;
}

/// The transaction a table op names, or null when it names none.
pub fn txnId(req: proto.Request) ?u64 {
    inline for (table.op_table) |o| {
        if (o.code == req.header.op_code) {
            const field = o.txn orelse return null;
            var diag: schema.Diagnostic = .{};
            const body = o.request.decodeAll(req.value, &diag) catch return null;
            return @field(body, field);
        }
    }
    return null;
}

/// Whether a table op goes to the leader: a write, or any op inside a
/// transaction, which lives on its leader.
pub fn writes(req: proto.Request) bool {
    inline for (table.op_table) |o| {
        if (o.code == req.header.op_code) return o.isWrite() or txnId(req) != null;
    }
    return false;
}

// ── Gates ──────────────────────────────────────────────────────────────

/// The test that exercises each request field of each table op, by its
/// name. A field missing here fails the build's tests, and so does a name
/// no test has any more.
pub const coverage = [_]struct { op: []const u8, field: []const u8, tests: []const []const u8 }{
    .{ .op = "kv.get", .field = "wait_ms", .tests = &.{"kv table: wait_ms with wait_for exists answers at once when the key exists, else when it is written"} },
    .{ .op = "kv.get", .field = "wait_for", .tests = &.{"kv table: wait_for change waits for the version after the one there"} },
    .{ .op = "kv.get", .field = "routing_key", .tests = &.{"kv table: routing_key routes the request in place of its key"} },
    .{ .op = "kv.get", .field = "txn", .tests = &.{
        "kv table: a set in a transaction is buffered, read back inside it, and unseen outside until it commits",
        "kv table: a forged or stale transaction id is refused as no such transaction",
        "kv table: a request in a transaction goes to the leader, and to the shard its id names",
    } },
    .{ .op = "kv.set", .field = "value", .tests = &.{"kv table: a set answers its version, and a get answers the value and that version"} },
    .{ .op = "kv.set", .field = "ttl_ms", .tests = &.{"kv table: ttl_ms is written with the value, get answers when it runs out, and none means no expiry"} },
    .{ .op = "kv.set", .field = "condition", .tests = &.{"kv table: condition if_absent and if_present refuse, unrun, when they don't hold"} },
    .{ .op = "kv.set", .field = "cas_version", .tests = &.{"kv table: cas_version must be the key's version, and 0 means absent"} },
    .{ .op = "kv.set", .field = "routing_key", .tests = &.{"kv table: routing_key routes the request in place of its key"} },
    .{ .op = "kv.set", .field = "txn", .tests = &.{
        "kv table: a set in a transaction is buffered, read back inside it, and unseen outside until it commits",
        "kv table: a forged or stale transaction id is refused as no such transaction",
    } },
    .{ .op = "kv.delete", .field = "cas_version", .tests = &.{"kv ops: delete answers whether the key existed, and cas_version guards it"} },
    .{ .op = "kv.delete", .field = "routing_key", .tests = &.{"kv table: routing_key routes the request in place of its key"} },
    .{ .op = "kv.delete", .field = "txn", .tests = &.{"kv ops: delete, incr, expire and exists in a transaction are buffered, and commit writes them as one"} },
    .{ .op = "kv.scan", .field = "limit", .tests = &.{"kv ops: scan pages names by limit and cursor, under a prefix, without reserved keys"} },
    .{ .op = "kv.scan", .field = "cursor", .tests = &.{"kv ops: scan pages names by limit and cursor, under a prefix, without reserved keys"} },
    .{ .op = "kv.history", .field = "routing_key", .tests = &.{"kv table: routing_key routes the request in place of its key"} },
    .{ .op = "kv.history", .field = "limit", .tests = &.{"kv ops: history answers versions newest first, at most limit; a missing key is not found"} },
    .{ .op = "kv.history", .field = "before_version", .tests = &.{"kv ops: history pages below before_version, and more says older versions follow"} },
    .{ .op = "kv.mget", .field = "routing_key", .tests = &.{ "Shard: kv.mget with a routing key is answered on that key's shard; without one, each key is found on its own", "kv table: kv.mget routes on its routing key, and stays where it lands without one" } },
    .{ .op = "kv.mget", .field = "keys", .tests = &.{"kv ops: mget answers each key in order, found or not, and next when they don't all fit"} },
    .{ .op = "kv.incr", .field = "delta", .tests = &.{"kv ops: incr adds delta to a counter from 0, and refuses a value that isn't one or a sum past i64"} },
    .{ .op = "kv.incr", .field = "routing_key", .tests = &.{"kv table: routing_key routes the request in place of its key"} },
    .{ .op = "kv.incr", .field = "txn", .tests = &.{"kv ops: delete, incr, expire and exists in a transaction are buffered, and commit writes them as one"} },
    .{ .op = "kv.expire", .field = "ttl_ms", .tests = &.{"kv ops: expire sets a TTL from now, 0 clears it, and cas_version guards it; a missing key is not found"} },
    .{ .op = "kv.expire", .field = "cas_version", .tests = &.{"kv ops: expire sets a TTL from now, 0 clears it, and cas_version guards it; a missing key is not found"} },
    .{ .op = "kv.expire", .field = "routing_key", .tests = &.{"kv table: routing_key routes the request in place of its key"} },
    .{ .op = "kv.expire", .field = "txn", .tests = &.{"kv ops: delete, incr, expire and exists in a transaction are buffered, and commit writes them as one"} },
    .{ .op = "kv.exists", .field = "routing_key", .tests = &.{"kv table: routing_key routes the request in place of its key"} },
    .{ .op = "kv.exists", .field = "txn", .tests = &.{"kv ops: delete, incr, expire and exists in a transaction are buffered, and commit writes them as one"} },
    .{ .op = "kv.json.get", .field = "path", .tests = &.{"kv ops: json_get, json_set and json_del work at a path, and $ is the whole document"} },
    .{ .op = "kv.json.get", .field = "routing_key", .tests = &.{"kv table: routing_key routes the request in place of its key"} },
    .{ .op = "kv.json.set", .field = "path", .tests = &.{"kv ops: json_get, json_set and json_del work at a path, and $ is the whole document"} },
    .{ .op = "kv.json.set", .field = "document", .tests = &.{"kv ops: json_get, json_set and json_del work at a path, and $ is the whole document"} },
    .{ .op = "kv.json.set", .field = "routing_key", .tests = &.{"kv table: routing_key routes the request in place of its key"} },
    .{ .op = "kv.json.del", .field = "path", .tests = &.{"kv ops: json_get, json_set and json_del work at a path, and $ is the whole document"} },
    .{ .op = "kv.json.del", .field = "routing_key", .tests = &.{"kv table: routing_key routes the request in place of its key"} },
    .{ .op = "kv.txn.commit", .field = "txn", .tests = &.{ "kv ops: delete, incr, expire and exists in a transaction are buffered, and commit writes them as one", "kv ops: rollback drops a transaction's writes, and then neither rollback nor commit finds it" } },
    .{ .op = "kv.txn.rollback", .field = "txn", .tests = &.{"kv ops: rollback drops a transaction's writes, and then neither rollback nor commit finds it"} },
    .{ .op = "worker.list", .field = "limit", .tests = &.{ "worker.list: pages over shards whose ids interleave give every worker once, in id order, whatever the limit", "Shard: worker.list on a node of two shards lists both shards' workers, in id order and paged" } },
    .{ .op = "worker.list", .field = "cursor", .tests = &.{ "worker.list: pages over shards whose ids interleave give every worker once, in id order, whatever the limit", "Shard: worker.list on a node of two shards lists both shards' workers, in id order and paged" } },
    .{ .op = "namespace.list", .field = "limit", .tests = &.{"namespace handler: list pages in name order, holding default from the start"} },
    .{ .op = "namespace.list", .field = "cursor", .tests = &.{"namespace handler: list pages in name order, holding default from the start"} },
    .{ .op = "worker.list", .field = "action", .tests = &.{"worker.list: an action filter pages only that action's workers, each page full, to the last"} },
};

/// The files that bind table ops' functions: where `_ = field;` would
/// drop a decoded field unread.
const handler_files = [_][]const u8{ "src/kv/handler.zig", "src/worker/handler.zig", "src/namespace/handler.zig" };

test "table_run: every request field of every table op has a test" {
    inline for (table.op_table) |o| {
        inline for (o.request.field_list) |f| {
            const listed = for (coverage) |c| {
                if (std.mem.eql(u8, c.op, o.name) and std.mem.eql(u8, c.field, f.name)) break true;
            } else false;
            if (!listed) std.debug.print("{s}.{s} has no test in table_run.coverage\n", .{ o.name, f.name });
            try std.testing.expect(listed);
        }
    }
    // And nothing listed that the table doesn't have.
    for (coverage) |c| {
        const known = inline for (table.op_table) |o| {
            if (std.mem.eql(u8, o.name, c.op)) break fieldIn(o.request.field_list, c.field);
        } else false;
        if (!known) std.debug.print("table_run.coverage lists {s}.{s}, which the table doesn't have\n", .{ c.op, c.field });
        try std.testing.expect(known);
    }
}

fn fieldIn(comptime fields: anytype, name: []const u8) bool {
    inline for (fields) |f| if (std.mem.eql(u8, f.name, name)) return true;
    return false;
}

test "table_run: every test the coverage list names exists" {
    const a = std.testing.allocator;
    const stdx = @import("stdx");
    var sources: std.ArrayListUnmanaged(u8) = .empty;
    defer sources.deinit(a);
    var stack: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (stack.items) |p| a.free(p);
        stack.deinit(a);
    }
    try stack.append(a, try a.dupe(u8, "src"));
    var i: usize = 0;
    while (i < stack.items.len) : (i += 1) {
        var dir = try stdx.fs.openDir(stack.items[i], .{ .iterate = true });
        defer stdx.fs.closeDir(dir);
        var it = dir.iterate();
        while (try it.next(stdx.io.instance())) |entry| {
            const path = try std.fs.path.join(a, &.{ stack.items[i], entry.name });
            if (entry.kind == .directory) {
                try stack.append(a, path);
                continue;
            }
            defer a.free(path);
            if (!std.mem.endsWith(u8, entry.name, ".zig")) continue;
            const source = try stdx.fs.readFileAlloc(a, path, 4 << 20);
            defer a.free(source);
            try sources.appendSlice(a, source);
        }
    }
    var missing: usize = 0;
    for (coverage) |c| for (c.tests) |name| {
        const decl = try std.fmt.allocPrint(a, "test \"{s}\"", .{name});
        defer a.free(decl);
        if (std.mem.indexOf(u8, sources.items, decl) == null) {
            std.debug.print("table_run.coverage names a test that doesn't exist: \"{s}\"\n", .{name});
            missing += 1;
        }
    };
    try std.testing.expectEqual(@as(usize, 0), missing);
}

test "table_run: no table op's function drops a decoded field with `_ =`" {
    const a = std.testing.allocator;
    const stdx = @import("stdx");
    var found: usize = 0;
    for (handler_files) |path| {
        const source = try stdx.fs.readFileAlloc(a, path, 4 << 20);
        defer a.free(source);
        const dense = try a.alloc(u8, source.len);
        defer a.free(dense);
        var n: usize = 0;
        for (source) |ch| if (!std.ascii.isWhitespace(ch)) {
            dense[n] = ch;
            n += 1;
        };
        inline for (table.op_table) |o| inline for (o.request.field_list) |f| {
            if (std.mem.indexOf(u8, dense[0..n], "_=" ++ f.name ++ ";") != null) {
                std.debug.print("{s} drops {s}'s field {s} with `_ =`; use it, or take it out of the schema\n", .{ path, o.name, f.name });
                found += 1;
            }
        };
    }
    try std.testing.expectEqual(@as(usize, 0), found);
}
