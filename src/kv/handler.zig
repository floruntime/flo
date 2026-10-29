//! KV Handler — registers KV opcodes with Dispatcher and handles KV operations.
//!
//! Read operations (get, scan, history) query the KV projection directly.
//! Write operations (put, delete) go through the Raft propose pipeline:
//!   1. Build CommandPayload from the request (key + value + TTL)
//!   2. Propose the entry to RaftNode
//!   3. `Shard.park` the request until the entry commits and is applied
//!      (at once with no peers, after a quorum acks otherwise)
//!   4. The client is answered from what the entry's applier recorded
//!
//! ## Handler Registration
//!
//! ```zig
//! var handler = KVHandler.init(allocator, &partition.kv);
//! handler.register(&dispatcher);
//! ```
//!
//! ## Dispatch Flow
//!
//! Acceptor → Shard → Dispatcher → KVHandler.dispatch{Get,Put,...}
//!   → [writes] raft_node.propose() → Shard.park() → applied → respond*()
//!   → [reads]  projection.get/scan() → sendResponse
//!
//! ## Reserved Keys
//!
//! Keys prefixed with `_action:`, `_worker:`, `_sys:`, `_internal:`, `_flo:`
//! are system-owned and blocked from user operations.

const std = @import("std");
const time_units = @import("../util/time_units.zig");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const kv_state = @import("state.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const waiter_pool_mod = @import("../node/waiter_pool.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const network_mode = @import("../raft/network.zig");
const ns_keys = @import("../namespace/handler.zig");
const router = @import("../node/router.zig");
const persistence_mod = @import("../storage/persistence.zig");
const Result = persistence_mod.Result;
const txn_mod = @import("./txn.zig");
const kv_ops = @import("../protocol/kv_ops.zig");
const ops = @import("../protocol/ops.zig");
const schema = @import("../protocol/schema.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const json_path = @import("../util/json_path.zig");
const json_depth = @import("../util/json_depth.zig");
const table_run = @import("../node/table_run.zig");
const mget_mod = @import("mget.zig");
const gather_mod = @import("../node/gather.zig");
const log = @import("stdx").log;
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;

const KvState = kv_state.KvState;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const OptionsBuilder = proto.OptionsBuilder;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const RaftNetwork = network_mode.RaftNetwork;

/// The largest single-key KV entry, a put: what `proposeKV` borrows
/// scratch for. A transaction's batch is larger and allocated.
pub const MAX_KEY_ENTRY_PAYLOAD: usize = kv_entries.Put.max_size;

/// Re-export from centralized namespace module for local convenience.
const MAX_QUALIFIED_KEY = ns_keys.MAX_QUALIFIED_KEY;
const qualifyKey = ns_keys.qualifyKey;
const stripNsPrefix = ns_keys.stripPrefix;
const nsPrefix = ns_keys.namespacePrefix;

/// Reserved key prefixes — operations on these are blocked for user requests.
const RESERVED_PREFIXES = [_][]const u8{
    "_action:",
    "_worker:",
    "_sys:",
    "_internal:",
    "_flo:",
};

// ═══════════════════════════════════════════════════════════════════════════════
// KVHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const KVHandler = struct {
    // ── Fields ──────────────────────────────────────────────────────────

    kv: *KvState,
    allocator: Allocator,

    /// Global metrics registry (optional, set by runtime when dashboard is enabled).
    metrics_registry: ?*MetricsRegistry,

    /// Per-shard transaction table. Lives next to the projection — same
    /// thread, no locks. Tracks open BEGIN'd txns until COMMIT/ROLLBACK.
    txn_table: txn_mod.TxnTable,

    /// Where a table op's answer is built when it outlives the call that
    /// makes it (a list, a JSON result): reused, one request at a time.
    answer_buf: [ops.MAX_RESPONSE_BODY]u8 = undefined,
    /// The cursor a scan answers with.
    cursor_buf: [kv_ops.MAX_CURSOR]u8 = undefined,

    pub fn init(allocator: Allocator, kv: *KvState) KVHandler {
        return .{
            .kv = kv,
            .allocator = allocator,
            .metrics_registry = null,
            .txn_table = txn_mod.TxnTable.init(allocator),
        };
    }

    pub fn deinit(self: *KVHandler) void {
        self.txn_table.deinit();
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    /// Register KV opcode handlers with the Dispatcher.
    /// Uses pre-route hooks for routing key extraction.
    pub fn register(dispatcher: *Dispatcher) void {
        inline for (.{
            .{ "kv.get", OpCode.kv_get, get },
            .{ "kv.set", OpCode.kv_put, set },
            .{ "kv.delete", OpCode.kv_delete, delete },
            .{ "kv.history", OpCode.kv_history, history },
            .{ "kv.incr", OpCode.kv_incr, incr },
            .{ "kv.expire", OpCode.kv_expire, expire },
            .{ "kv.exists", OpCode.kv_exists, exists },
            .{ "kv.json.get", OpCode.kv_json_get, jsonGet },
            .{ "kv.json.set", OpCode.kv_json_set, jsonSet },
            .{ "kv.json.del", OpCode.kv_json_del, jsonDel },
            .{ "kv.txn.begin", OpCode.kv_begin_txn, txnBegin },
            .{ "kv.txn.commit", OpCode.kv_commit_txn, txnCommit },
            .{ "kv.txn.rollback", OpCode.kv_rollback_txn, txnRollback },
        }) |o| {
            dispatcher.registerWithRoute(o[1], table_run.handler(o[0], KVHandler, o[2]), table_run.routeByKey(o[0]));
            // A request in a transaction runs on the shard its id names.
            if (table_run.op(o[0]).txn != null) dispatcher.setShardRoute(o[1], txnShardOfBody);
        }
        // Many keys: on the routing key's shard when given, else where the
        // client is.
        dispatcher.registerWithRoute(.kv_mget, table_run.handler("kv.mget", KVHandler, mget), table_run.routeByField("kv.mget"));
        // Every shard: walked from the client's.
        dispatcher.registerWalk(.kv_scan, table_run.handler("kv.scan", KVHandler, scan), localScanKeys);
    }

    // ── Table ops ───────────────────────────────────────────────────────
    // `table_run.handler` decodes each body and calls these with its fields.

    /// Why a KV table op is refused. `refusal` gives each its reason.
    pub const Error = error{
        KeyEmpty,
        KeyHasNul,
        KeyTooLong,
        ReservedKey,
        NotFound,
        CasMismatch,
        ConditionFailed,
        NoSuchTransaction,
        WrongRoutingKey,
        TxnTooLarge,
        WaitInTxn,
        TooManyWaiters,
        LeaderUnknown,
        WritesStopped,
        TooManyProposals,
        TooLarge,
        AnswerTooLarge,
        NotACounter,
        CounterOverflow,
        PathNotFound,
        BadPath,
        NotADocument,
        NotJson,
        TooDeep,
        TooManyTransactions,
        ScanShort,
        NotOwner,
        GatherBusy,
        Internal,
    };

    pub fn refusal(err: Error) table_run.Refused {
        return switch (err) {
            error.KeyEmpty => refused(.bad_request, .malformed, "bad request: key is required"),
            error.KeyHasNul => refused(.bad_request, .malformed, "bad request: key must not contain NUL"),
            error.KeyTooLong => refused(.bad_request, .field_too_long, std.fmt.comptimePrint("bad request: key is too long: at most {d} bytes, namespace included", .{MAX_QUALIFIED_KEY})),
            error.ReservedKey => refused(.unauthorized, .reserved_key, "unauthorized: the key is in a prefix the server keeps for itself"),
            error.NotFound => refused(.not_found, .not_found, "not found"),
            error.CasMismatch => refused(.conflict, .cas_mismatch, "conflict: cas_version isn't the key's version"),
            error.ConditionFailed => refused(.conflict, .condition_failed, "conflict: the condition didn't hold"),
            error.NoSuchTransaction => refused(.not_found, .no_such_transaction, "not found: no such transaction — it committed, rolled back, or was begun in another namespace"),
            error.WrongRoutingKey => refused(.bad_request, .wrong_routing_key, "bad request: the routing key (the key, when none is given) isn't the one the transaction was begun with"),
            error.TxnTooLarge => refused(.bad_request, .txn_too_large, "bad request: the transaction holds as many ops or bytes as it may"),
            error.WaitInTxn => refused(.bad_request, .conflicting_fields, "bad request: a read in a transaction can't wait: give wait_ms or txn, not both"),
            error.TooManyWaiters => refused(.overloaded, .too_many_waiters, "overloaded: too many reads waiting on this shard — retry"),
            error.LeaderUnknown => refused(.unavailable, .leader_unknown, persistence_mod.failureMessage(error.NotLeader, "")),
            error.WritesStopped => refused(.unavailable, .writes_stopped, persistence_mod.failureMessage(error.WritesStopped, "")),
            error.TooManyProposals => refused(.overloaded, .too_many_proposals, persistence_mod.failureMessage(error.Overloaded, "")),
            error.AnswerTooLarge => refused(.bad_request, .field_too_long, std.fmt.comptimePrint("bad request: the JSON at that path is more than an answer holds: at most {d} bytes", .{kv_ops.MAX_VALUE})),
            error.TooLarge => refused(.bad_request, .field_too_long, std.fmt.comptimePrint("bad request: the value to write is more than a key holds: at most {d} bytes", .{kv_ops.MAX_VALUE})),
            error.NotACounter => refused(.bad_request, .wrong_type, "bad request: the key's value isn't an 8-byte counter"),
            error.CounterOverflow => refused(.bad_request, .out_of_range, "bad request: delta would take the counter past an i64"),
            error.PathNotFound => refused(.not_found, .not_found, "not found: nothing at that path in the document"),
            error.BadPath => refused(.bad_request, .malformed, "bad request: path isn't a JSON path"),
            error.NotADocument => refused(.bad_request, .wrong_type, "bad request: the key's value isn't a JSON document"),
            error.NotJson => refused(.bad_request, .malformed, "bad request: document isn't JSON"),
            error.TooDeep => refused(.bad_request, .field_too_long, "bad request: " ++ json_depth.TOO_DEEP_MESSAGE),
            error.TooManyTransactions => refused(.overloaded, .too_many_transactions, "overloaded: too many open transactions on this shard — commit or roll some back, or retry"),
            error.ScanShort => refused(.overloaded, .shard_busy, "overloaded: no memory to list this page — retry"),
            error.GatherBusy => refused(.overloaded, .shard_busy, "overloaded: too many reads across shards are under way; this request was not run — retry"),
            error.NotOwner => refused(.internal_error, .internal, "internal error: a shard was asked for a key another shard owns"),
            error.Internal => refused(.internal_error, .internal, "internal error: the request couldn't be run"),
        };
    }

    /// Refused before anything ran.
    fn refused(status: proto.StatusCode, reason: proto.Reason, message: []const u8) table_run.Refused {
        return .{ .status = status, .refusal = .{ .reason = reason, .ran = .no, .message = message } };
    }

    comptime {
        // A wait the schema allows is one a waiter keeps.
        std.debug.assert(kv_ops.MAX_WAIT_MS == waiter_pool_mod.MAX_BLOCK_MS);
    }

    pub const GetResponse = table_run.op("kv.get").response;
    const SetResponse = table_run.op("kv.set").response;

    /// Read a key: at once, or once it exists or changes (`wait_ms`,
    /// `wait_for`), or as a transaction sees it.
    fn get(
        shard: *Shard,
        conn: *Connection,
        req: Request,
        wait_ms: u64,
        wait_for: kv_ops.WaitFor,
        routing_key: ?[]const u8,
        txn: ?u64,
    ) Error!table_run.Outcome(GetResponse) {
        const in_txn = try txnOf(shard, req, txn, routing_key);
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = try tableKey(&qbuf, req);
        const kv = shard.kv_handler.*.kv;
        if (in_txn) |t| {
            if (wait_ms > 0) return error.WaitInTxn;
            if (shard.kv_handler.*.txn_table.lastOpForKey(t.txn_id, qkey)) |last| switch (last.kind) {
                .delete => return error.NotFound,
                // Not committed, so no version or expiry yet.
                .put => return .{ .answer = .{ .version = 0, .value = last.value, .expires_at_ms = null } },
                // Their value isn't the key's: read what's committed.
                .expire, .incr => {},
            };
        }
        const current = kv.get(qkey);
        if (wait_ms > 0) {
            if (wait_for == .exists) if (current) |e| return .{ .answer = getAnswer(e) };
            const registered = shard.waiter_pool.register(.{
                .kind = .kv_get,
                .reply_to = conn.replyTo(),
                .request_id = req.header.request_id,
                .key = qkey,
                // Answered by the first version past this one.
                .min_version = switch (wait_for) {
                    .exists => 0,
                    .change => if (current) |e| e.version else 0,
                },
                // At most kv_ops.MAX_WAIT_MS: the decode checked.
                .timeout_ms = @intCast(wait_ms),
            });
            if (!registered) return error.TooManyWaiters;
            return .wait;
        }
        if (shard.kv_handler.metrics_registry) |mr| {
            if (mr.registerKVNamespace(req.namespace)) |km| km.recordGet() else |_| {}
        }
        const e = current orelse return error.NotFound;
        return .{ .answer = getAnswer(e) };
    }

    /// A committed key as kv.get answers it, here and from a waiting get.
    pub fn getAnswer(e: kv_state.Current) GetResponse.Value {
        return .{
            .version = e.version,
            .value = e.value,
            .expires_at_ms = if (e.expiry_ns > 0) @intCast(e.expiry_ns / std.time.ns_per_ms) else null,
        };
    }

    /// Write a key, if its `condition` and `cas_version` hold; inside a
    /// transaction, buffer the write until it commits.
    fn set(
        shard: *Shard,
        _: *Connection,
        req: Request,
        value: []const u8,
        ttl_ms: ?u64,
        condition: kv_ops.Condition,
        cas_version: ?u64,
        routing_key: ?[]const u8,
        txn: ?u64,
    ) Error!table_run.Outcome(SetResponse) {
        const in_txn = try txnOf(shard, req, txn, routing_key);
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = try tableKey(&qbuf, req);
        const current = shard.kv_handler.*.kv.get(qkey);
        if (cas_version) |want| if ((if (current) |e| e.version else 0) != want) return error.CasMismatch;
        switch (condition) {
            .always => {},
            .if_absent => if (current != null) return error.ConditionFailed,
            .if_present => if (current == null) return error.ConditionFailed,
        }
        const ttl = ttl_ms orelse 0;
        if (in_txn) |t| {
            try bufferInTxn(shard, t, qkey, .put, value, ttl);
            return .{ .answer = .{ .version = null } };
        }
        const proposed = proposeKV(shard, .kv_put, .{ .key = qkey, .value = value, .ttl_ms = if (ttl == 0) null else ttl }) catch |err| return proposeError(err);
        shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        return .{ .park = .{ .proposed = proposed, .answer = answerSet } };
    }

    /// The set applied: answered with the version its applier gave the key.
    fn answerSet(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        table_run.answer(SetResponse, shard, conn, request_id, .{ .version = r.int(u64) orelse 0 });
    }

    /// The request's key, namespace-qualified, once it passes the checks
    /// every key does.
    fn tableKey(buf: *[MAX_QUALIFIED_KEY]u8, req: Request) Error![]const u8 {
        if (req.key.len == 0) return error.KeyEmpty;
        if (isReservedKey(req.key)) return error.ReservedKey;
        return qualifyKey(buf, req.namespace, req.key) catch |err| switch (err) {
            error.KeyHasNul => error.KeyHasNul,
            error.KeyTooLarge => error.KeyTooLong,
        };
    }

    /// Transaction `id`, open in the request's namespace, which the
    /// request's key routes to.
    fn openTxn(shard: *Shard, req: Request, id: u64, routing_key: ?[]const u8) Error!*txn_mod.TxnState {
        const t = txnIn(&shard.kv_handler.*.txn_table, id, req.namespace) orelse return error.NoSuchTransaction;
        if (t.pinned_hash != router.hashKeyWithNamespace(req.namespace, routing_key orelse req.key)) return error.WrongRoutingKey;
        return t;
    }

    /// The transaction a request names, if any. Looked up before any other
    /// check: a request refused for its key or condition still uses the
    /// transaction, and one that is gone is answered as gone.
    fn txnOf(shard: *Shard, req: Request, txn: ?u64, routing_key: ?[]const u8) Error!?*txn_mod.TxnState {
        return if (txn) |id| try openTxn(shard, req, id, routing_key) else null;
    }

    fn proposeError(err: anyerror) Error {
        return switch (err) {
            error.NotLeader => error.LeaderUnknown,
            error.WritesStopped => error.WritesStopped,
            error.Overloaded => error.TooManyProposals,
            error.PayloadTooLarge => error.TooLarge,
            else => error.Internal,
        };
    }

    /// The shard a table op's transaction lives on, when it names one.
    fn txnShardOfBody(req: Request) ?u16 {
        return txn_mod.idShard(table_run.txnId(req) orelse return null);
    }

    const DeleteResponse = table_run.op("kv.delete").response;
    const ScanResponse = table_run.op("kv.scan").response;
    const HistoryResponse = table_run.op("kv.history").response;
    const MgetResponse = table_run.op("kv.mget").response;
    const IncrResponse = table_run.op("kv.incr").response;
    const ExpireResponse = table_run.op("kv.expire").response;
    const ExistsResponse = table_run.op("kv.exists").response;
    const JsonGetResponse = table_run.op("kv.json.get").response;
    const JsonSetResponse = table_run.op("kv.json.set").response;
    const JsonDelResponse = table_run.op("kv.json.del").response;
    const TxnBeginResponse = table_run.op("kv.txn.begin").response;
    const TxnCommitResponse = table_run.op("kv.txn.commit").response;
    const TxnRollbackResponse = table_run.op("kv.txn.rollback").response;

    /// Delete a key, if `cas_version` holds; deleting one that doesn't
    /// exist succeeds. Inside a transaction, buffer the delete.
    fn delete(
        shard: *Shard,
        _: *Connection,
        req: Request,
        cas_version: ?u64,
        routing_key: ?[]const u8,
        txn: ?u64,
    ) Error!table_run.Outcome(DeleteResponse) {
        const in_txn = try txnOf(shard, req, txn, routing_key);
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = try tableKey(&qbuf, req);
        const current = shard.kv_handler.*.kv.get(qkey);
        if (cas_version) |want| if ((if (current) |e| e.version else 0) != want) return error.CasMismatch;
        if (in_txn) |t| {
            try bufferInTxn(shard, t, qkey, .delete, "", 0);
            return .{ .answer = .{ .existed = null } };
        }
        if (current == null) return .{ .answer = .{ .existed = false } };
        const proposed = proposeKV(shard, .kv_delete, .{ .key = qkey }) catch |err| return proposeError(err);
        return .{ .park = .{ .proposed = proposed, .answer = answerDeleted } };
    }

    /// Whether the key was there when the delete applied: another delete
    /// can apply first.
    fn answerDeleted(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        table_run.answer(DeleteResponse, shard, conn, request_id, .{ .existed = (r.int(u8) orelse 0) == 1 });
    }

    /// The names of keys starting with the request's key, a page at a time
    /// across every shard. A walk runs from the shard the client is on; a
    /// node whose walk isn't wired yet lists its own shard.
    fn scan(
        shard: *Shard,
        _: *Connection,
        req: Request,
        limit: u32,
        cursor: []const u8,
    ) Error!table_run.Outcome(ScanResponse) {
        const NameWalker = dispatcher_mod.NameWalker;
        const self = shard.kv_handler;
        const local = [_]*anyopaque{@ptrCast(self.kv)};
        const contexts = shard.dispatcher.walk_contexts[@backingInt(OpCode.kv_scan)] orelse &local;
        const walker = NameWalker.init(localScanKeys, @intCast(contexts.len));
        var found: [NameWalker.MAX_BATCH][]const u8 = undefined;
        const from = if (cursor.len == 0) null else cursor;
        var want: u32 = @min(limit, NameWalker.MAX_BATCH);
        const Names = ScanResponse.field_list[0].T;
        while (true) {
            _ = dispatcher_mod.takeScanShort();
            const page = walker.walk(contexts, req.namespace, req.key, from, want, &found, &self.cursor_buf);
            if (dispatcher_mod.takeScanShort()) return error.ScanShort;

            var names = Names.Builder.init(&self.answer_buf);
            const full: ?usize = for (page.items, 0..) |name, i| {
                // A name two shards both hold is listed once.
                for (page.items[0..i]) |earlier| {
                    if (std.mem.eql(u8, earlier, name)) break;
                } else if (!(names.add(name) catch return error.Internal)) break i;
            } else null;
            // The page's cursor resumes after every name walked, so a page
            // the answer can't hold is walked again, short enough to fit:
            // the same first names, and a cursor after the last one sent.
            if (full) |fit| {
                std.debug.assert(fit > 0 and fit < want);
                want = @intCast(fit);
                continue;
            }
            return .{ .answer = .{ .names = names.value(), .cursor = page.next_cursor } };
        }
    }

    /// A key's versions, newest first, below `before_version` when given,
    /// as many as `limit` and one answer hold; `more` says whether older
    /// kept versions follow. A key with no history at all is not found.
    fn history(shard: *Shard, _: *Connection, req: Request, limit: u32, before_version: ?u64, _: ?[]const u8) Error!table_run.Outcome(HistoryResponse) {
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = try tableKey(&qbuf, req);
        const self = shard.kv_handler;
        var it = self.kv.history(qkey, before_version);
        if (!it.exists()) return error.NotFound;
        const Versions = HistoryResponse.field_list[0].T;
        var versions = Versions.Builder.init(&self.answer_buf);
        var shown: u32 = 0;
        var more = false;
        while (it.next()) |v| {
            const at_ms: i64 = @intCast(v.timestamp_ns / std.time.ns_per_ms);
            if (shown == limit or !(versions.add(.{ .version = v.version, .at_ms = at_ms, .deleted = v.tombstone, .value = v.value }) catch return error.Internal)) {
                more = true;
                break;
            }
            shown += 1;
        }
        return .{ .answer = .{ .versions = versions.value(), .more = more } };
    }

    /// Many keys at once, in the order asked, as many as one answer holds;
    /// `next` is the first key left out.
    fn mget(shard: *Shard, conn: *Connection, req: Request, keys: MgetKeys.Value, routing_key: ?[]const u8) Error!table_run.Outcome(MgetResponse) {
        // Another shard asked this one for the keys it owns: a key that
        // routes elsewhere is refused, never read here or answered missing.
        const forwarded = conn.proxy_for != null;
        // Keys other shards own are read on those shards: each owner gets
        // its part, and the answers are merged here once all have come.
        if (routing_key == null and !forwarded and shard.shardCount() > 1) {
            var plan = mget_mod.plan(shard.allocator, keys, MgetOwner{ .shard = shard, .namespace = req.namespace }, MgetOwner.of) catch return error.Internal;
            defer plan.deinit(shard.allocator);
            if (plan.owners.len > 1 or (plan.owners.len == 1 and plan.owners[0] != shard.id)) {
                try startMgetGather(shard, conn, req, &plan);
                return .wait;
            }
        }
        const self = shard.kv_handler;
        const Entries = MgetResponse.field_list[0].T;
        var entries = Entries.Builder.init(&self.answer_buf);
        var it = keys.iterator();
        var index: u16 = 0;
        while (it.next() catch return error.Internal) |raw_key| : (index += 1) {
            const found = try mgetLookup(shard, req.namespace, raw_key, routing_key != null);
            if (!(entries.add(.{ .found = found.found, .version = found.version, .value = found.value }) catch return error.Internal)) {
                return .{ .answer = .{ .entries = entries.value(), .next = index } };
            }
        }
        return .{ .answer = .{ .entries = entries.value(), .next = null } };
    }

    /// One key of an mget, from this shard's own map. A key that can't be
    /// one, or one the server keeps, is not found. Without a routing key the
    /// key must be this shard's: one another shard owns is never read here
    /// or answered missing.
    fn mgetLookup(shard: *Shard, namespace: []const u8, raw_key: []const u8, routed: bool) Error!MgetLookup {
        const none: MgetLookup = .{ .found = false, .value = "", .version = 0 };
        if (raw_key.len == 0 or isReservedKey(raw_key)) return none;
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, namespace, raw_key) catch return none;
        // With a routing key the request was routed to its shard, where
        // every key lives.
        if (!routed) {
            const owner = MgetOwner.of(.{ .shard = shard, .namespace = namespace }, raw_key);
            if (owner != shard.id) {
                log.err("shard {d}: kv.mget was asked for key \"{s}\", which shard {d} owns", .{ shard.id, raw_key, owner });
                return error.NotOwner;
            }
        }
        const entry = shard.kv_handler.*.kv.get(qkey) orelse return none;
        return .{ .found = true, .value = entry.value, .version = entry.version };
    }

    /// Ask each owner for its keys, as a kv.mget of them, and answer from
    /// their answers in the order asked. This shard's own keys are answered
    /// here, as the body another owner would send.
    fn startMgetGather(shard: *Shard, conn: *Connection, req: Request, plan: *const mget_mod.Plan) Error!void {
        // The other owners' bodies end to end, sized exactly; the gather
        // copies them at the send.
        var size: usize = 0;
        for (plan.owners, plan.keys) |owner, part_keys| {
            if (owner != shard.id) size += mget_mod.pieceBodySize(part_keys) catch return error.Internal;
        }
        const scratch = shard.allocator.alloc(u8, size) catch return error.Internal;
        defer shard.allocator.free(scratch);
        var parts: [mget_mod.MAX_PIECES]gather_mod.Part = undefined;
        var used: usize = 0;
        var local: ?[]u8 = null;
        errdefer if (local) |b| shard.allocator.free(b);
        for (plan.owners, plan.keys, 0..) |owner, part_keys, i| {
            if (owner == shard.id) {
                local = try localPart(shard, req.namespace, part_keys);
                parts[i] = .{ .owner = owner, .body = local.? };
            } else {
                const body = mget_mod.pieceBody(scratch[used..], part_keys) catch unreachable; // sized above
                used += body.len;
                parts[i] = .{ .owner = owner, .body = body };
            }
        }
        const state = mget_mod.Gather.create(shard.allocator, plan.slots) catch return error.Internal;
        local = null; // the gather owns it from the call, failed or not
        shard.startGather(conn, req, parts[0..plan.owners.len], &mget_mod.Gather.op, state) catch |err| return switch (err) {
            error.GatherBusy => error.GatherBusy,
            error.OutOfMemory => error.Internal,
        };
    }

    /// The part of a gathered mget this shard owns, answered here as the
    /// body another owner would send.
    fn localPart(shard: *Shard, namespace: []const u8, keys: []const []const u8) Error![]u8 {
        const Entries = MgetResponse.field_list[0].T;
        var entries = Entries.Builder.init(&shard.kv_handler.answer_buf);
        var next: ?u16 = null;
        for (keys, 0..) |raw_key, i| {
            const found = try mgetLookup(shard, namespace, raw_key, false);
            if (!(entries.add(.{ .found = found.found, .version = found.version, .value = found.value }) catch return error.Internal)) {
                next = @intCast(i);
                break;
            }
        }
        return mget_mod.encodeBody(shard.allocator, .{ .entries = entries.value(), .next = next }) catch error.Internal;
    }

    /// Which shard owns a key of an mget: a key that can't be one, or one
    /// the server keeps, is answered where the request is.
    const MgetOwner = struct {
        shard: *const Shard,
        namespace: []const u8,

        fn of(self: MgetOwner, raw_key: []const u8) u16 {
            if (raw_key.len == 0 or isReservedKey(raw_key)) return self.shard.id;
            var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
            _ = qualifyKey(&qbuf, self.namespace, raw_key) catch return self.shard.id;
            const r = &self.shard.router;
            return r.partitionToShard(r.hashToPartition(router.hashKeyWithNamespace(self.namespace, raw_key)));
        }
    };

    const MgetKeys = table_run.op("kv.mget").request.field_list[0].T;

    /// Add `delta` to a key's 8-byte counter, which starts from 0. Inside a
    /// transaction, buffer it: the sum isn't known until commit.
    fn incr(
        shard: *Shard,
        _: *Connection,
        req: Request,
        delta: i64,
        routing_key: ?[]const u8,
        txn: ?u64,
    ) Error!table_run.Outcome(IncrResponse) {
        const in_txn = try txnOf(shard, req, txn, routing_key);
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = try tableKey(&qbuf, req);
        // A key whose TTL has run out is absent, as apply treats it.
        if (shard.kv_handler.*.kv.get(qkey)) |e| {
            if (e.value.len != 8) return error.NotACounter;
            _ = std.math.add(i64, std.mem.readInt(i64, e.value[0..8], .little), delta) catch return error.CounterOverflow;
        }
        var delta_bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &delta_bytes, delta, .little);
        if (in_txn) |t| {
            try bufferInTxn(shard, t, qkey, .incr, &delta_bytes, 0);
            return .{ .answer = .{ .version = null, .value = null } };
        }
        const proposed = proposeKV(shard, .kv_incr, .{ .key = qkey, .delta = delta }) catch |err| return proposeError(err);
        shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        return .{ .park = .{ .proposed = proposed, .answer = answerIncr } };
    }

    /// The counter applied: its version and value then. One that would
    /// overflow, or met a value that isn't a counter, was refused where it
    /// applied.
    fn answerIncr(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        const version = r.int(u64) orelse 0;
        table_run.answer(IncrResponse, shard, conn, request_id, .{ .version = version, .value = r.int(i64) orelse 0 });
    }

    /// Set a key's TTL, counted from this write, or clear it (`ttl_ms` 0),
    /// if `cas_version` holds. Inside a transaction, buffer it.
    fn expire(
        shard: *Shard,
        _: *Connection,
        req: Request,
        ttl_ms: u64,
        cas_version: ?u64,
        routing_key: ?[]const u8,
        txn: ?u64,
    ) Error!table_run.Outcome(ExpireResponse) {
        const in_txn = try txnOf(shard, req, txn, routing_key);
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = try tableKey(&qbuf, req);
        const current = shard.kv_handler.*.kv.get(qkey) orelse return error.NotFound;
        if (cas_version) |want| if (current.version != want) return error.CasMismatch;
        if (in_txn) |t| {
            try bufferInTxn(shard, t, qkey, .expire, "", ttl_ms);
            return .{ .answer = .{} };
        }
        const proposed = proposeKV(shard, .kv_expire, .{ .key = qkey, .ttl_ms = ttl_ms }) catch |err| return proposeError(err);
        return .{ .park = .{ .proposed = proposed, .answer = answerExpired } };
    }

    /// A key gone by the time the expire applied was refused there.
    fn answerExpired(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        table_run.answer(ExpireResponse, shard, conn, request_id, .{});
    }

    /// Whether a key exists, and its version; inside a transaction, as the
    /// transaction sees it (a buffered write has no version yet).
    fn exists(
        shard: *Shard,
        _: *Connection,
        req: Request,
        routing_key: ?[]const u8,
        txn: ?u64,
    ) Error!table_run.Outcome(ExistsResponse) {
        const in_txn = try txnOf(shard, req, txn, routing_key);
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = try tableKey(&qbuf, req);
        if (in_txn) |t| {
            if (shard.kv_handler.*.txn_table.lastOpForKey(t.txn_id, qkey)) |last| switch (last.kind) {
                .delete => return .{ .answer = .{ .exists = false, .version = 0 } },
                .put => return .{ .answer = .{ .exists = true, .version = 0 } },
                .expire, .incr => {},
            };
        }
        const e = shard.kv_handler.*.kv.get(qkey) orelse return .{ .answer = .{ .exists = false, .version = 0 } };
        return .{ .answer = .{ .exists = true, .version = e.version } };
    }

    /// The JSON at `path` in a key's document, with the document's version.
    fn jsonGet(
        shard: *Shard,
        _: *Connection,
        req: Request,
        path: []const u8,
        _: ?[]const u8,
    ) Error!table_run.Outcome(JsonGetResponse) {
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = try tableKey(&qbuf, req);
        const self = shard.kv_handler;
        const e = self.kv.get(qkey) orelse return error.NotFound;
        const at = json_path.jsonPathGet(self.allocator, e.value, path) catch |err| return jsonError(err);
        defer self.allocator.free(at);
        // The answer outlives this call; the handler's answer buffer holds it.
        if (at.len > kv_ops.MAX_VALUE) return error.AnswerTooLarge;
        @memcpy(self.answer_buf[0..at.len], at);
        return .{ .answer = .{ .version = e.version, .document = self.answer_buf[0..at.len] } };
    }

    /// Set the JSON at `path` in a key's document; `$` on a missing key
    /// writes the whole document.
    fn jsonSet(
        shard: *Shard,
        _: *Connection,
        req: Request,
        path: []const u8,
        document: []const u8,
        _: ?[]const u8,
    ) Error!table_run.Outcome(JsonSetResponse) {
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = try tableKey(&qbuf, req);
        const self = shard.kv_handler;
        if (!json_depth.within(document)) return error.TooDeep;
        if (!(std.json.validate(self.allocator, document) catch return error.Internal)) return error.NotJson;
        const merged: []u8 = if (self.kv.get(qkey)) |e|
            json_path.jsonPathSet(self.allocator, e.value, path, document) catch |err| return jsonError(err)
        else if (std.mem.eql(u8, path, "$"))
            self.allocator.dupe(u8, document) catch return error.Internal
        else
            return error.NotFound;
        defer self.allocator.free(merged);
        const proposed = proposeKV(shard, .kv_put, .{ .key = qkey, .value = merged, .ttl_ms = null }) catch |err| return proposeError(err);
        shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        return .{ .park = .{ .proposed = proposed, .answer = answerJsonSet } };
    }

    fn answerJsonSet(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        table_run.answer(JsonSetResponse, shard, conn, request_id, .{ .version = r.int(u64) orelse 0 });
    }

    /// Delete the JSON at `path`; `$` deletes the key.
    fn jsonDel(
        shard: *Shard,
        _: *Connection,
        req: Request,
        path: []const u8,
        _: ?[]const u8,
    ) Error!table_run.Outcome(JsonDelResponse) {
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = try tableKey(&qbuf, req);
        const self = shard.kv_handler;
        const e = self.kv.get(qkey) orelse return error.NotFound;
        if (std.mem.eql(u8, path, "$")) {
            const proposed = proposeKV(shard, .kv_delete, .{ .key = qkey }) catch |err| return proposeError(err);
            return .{ .park = .{ .proposed = proposed, .answer = answerJsonDeleted } };
        }
        const rest = json_path.jsonPathDel(self.allocator, e.value, path) catch |err| return jsonError(err);
        defer self.allocator.free(rest);
        const proposed = proposeKV(shard, .kv_put, .{ .key = qkey, .value = rest, .ttl_ms = null }) catch |err| return proposeError(err);
        return .{ .park = .{ .proposed = proposed, .answer = answerJsonPut } };
    }

    /// The path was deleted from the document: its version now.
    fn answerJsonPut(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        table_run.answer(JsonDelResponse, shard, conn, request_id, .{ .version = r.int(u64) orelse 0 });
    }

    /// `$` was deleted: the key went with it, and has no version.
    fn answerJsonDeleted(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        table_run.answer(JsonDelResponse, shard, conn, request_id, .{ .version = null });
    }

    fn jsonError(err: json_path.PathError) Error {
        return switch (err) {
            error.PathNotFound, error.NotAnObject, error.NotAnArray, error.IndexOutOfBounds => error.PathNotFound,
            error.InvalidPath => error.BadPath,
            error.InvalidJson => error.NotADocument,
            error.TooDeep => error.TooDeep,
            error.OutOfMemory => error.Internal,
        };
    }

    /// Begin a transaction pinned to the shard the request's key routes to:
    /// the key is the routing key every op in it gives.
    fn txnBegin(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(TxnBeginResponse) {
        if (req.key.len == 0) return error.KeyEmpty;
        const pinned = router.hashKeyWithNamespace(req.namespace, req.key);
        const id = shard.kv_handler.*.txn_table.begin(pinned, router.namespaceHash(req.namespace)) catch |err| return switch (err) {
            error.TooManyOpenTxns => error.TooManyTransactions,
            error.OutOfMemory => error.Internal,
        };
        return .{ .answer = .{ .txn = id, .pinned = pinned } };
    }

    /// Commit a transaction's writes as one log entry.
    fn txnCommit(shard: *Shard, _: *Connection, req: Request, txn: u64) Error!table_run.Outcome(TxnCommitResponse) {
        const self = shard.kv_handler;
        const t = txnIn(&self.txn_table, txn, req.namespace) orelse return error.NoSuchTransaction;
        if (t.ops.items.len == 0) {
            self.txn_table.drop(txn);
            return .{ .answer = .{ .commit_index = shard.raft_node.commit_index, .ops = 0 } };
        }
        const payload = txn_mod.encodeBatch(self.allocator, t.ops.items) catch return error.Internal;
        defer self.allocator.free(payload);
        const proposed = shard.raft_node.propose(.kv_batch, entry_mod.Flags.NONE, payload) catch |err| return proposeError(err);
        // The transaction stays until its batch is answered for, applied
        // or not; until then a second commit would propose it again. If the
        // batch applies after a failure was answered, it applies whole.
        t.committing = true;
        return .{ .park = .{ .proposed = proposed, .answer = answerCommitted, .release = dropTxn, .ctx = txn } };
    }

    /// The batch applied: its index, and the ops its applier counted.
    fn answerCommitted(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        table_run.answer(TxnCommitResponse, shard, conn, request_id, .{ .commit_index = result.index, .ops = r.int(u16) orelse 0 });
    }

    fn dropTxn(shard: *Shard, txn: u64) void {
        shard.kv_handler.*.txn_table.drop(txn);
    }

    /// Drop a transaction and its buffered writes.
    fn txnRollback(shard: *Shard, _: *Connection, req: Request, txn: u64) Error!table_run.Outcome(TxnRollbackResponse) {
        const table = &shard.kv_handler.*.txn_table;
        _ = txnIn(table, txn, req.namespace) orelse return error.NoSuchTransaction;
        table.drop(txn);
        return .{ .answer = .{} };
    }

    /// Buffer a write in transaction `t`.
    fn bufferInTxn(
        shard: *Shard,
        t: *txn_mod.TxnState,
        qkey: []const u8,
        kind: txn_mod.TxnOpKind,
        value: []const u8,
        ttl_ms: u64,
    ) Error!void {
        shard.kv_handler.*.txn_table.appendOp(t.txn_id, kind, qkey, value, ttl_ms) catch |err| return switch (err) {
            error.TxnTooLarge => error.TxnTooLarge,
            error.OutOfMemory => error.Internal,
        };
    }

    // ── Shard Walker: Local Scan ──────────────────────────────────────

    /// ShardWalker LocalScanFn for kv_scan — scans key names from one shard's
    /// KV state. Returns borrowed references (zero allocation).
    /// Filters reserved keys and strips namespace prefix.
    /// When filter is non-empty, only returns keys matching the prefix filter.
    fn localScanKeys(
        ctx: *anyopaque,
        namespace: []const u8,
        filter: []const u8,
        _: ?[]const u8,
        limit: u32,
    ) dispatcher_mod.NameWalker.ScanResult {
        const kv: *KvState = @ptrCast(@alignCast(ctx));
        const key_buf = dispatcher_mod.scanScratch(kv.totalEntries());
        // A key can't hold the separator, so a filter that does names nothing.
        if (std.mem.indexOfScalar(u8, filter, ns_keys.NAMESPACE_SEPARATOR) != null) return .{ .items = &.{}, .next_cursor = null };
        // Keys come back in key order and without their namespace.
        const ns = if (namespace.len == 0) "default" else namespace;
        const raw_count = kv.scanKeyNames(ns, filter, key_buf);
        var count: usize = 0;
        const cap: usize = if (limit > 0) @intCast(limit) else key_buf.len;
        for (key_buf[0..raw_count]) |key| {
            if (count >= cap) break;
            if (!isReservedKey(key)) {
                key_buf[count] = key;
                count += 1;
            }
        }

        return .{ .items = key_buf[0..count], .next_cursor = null };
    }

    const MgetLookup = struct {
        found: bool,
        value: []const u8,
        version: u64,
    };

    // ── Raft Propose ────────────────────────────────────────────────────

    /// Propose KV entry `entry_type` carrying `payload`. A value past what
    /// a read can answer is refused here, so nothing is stored that can't
    /// be read back.
    fn proposeKV(
        shard: *Shard,
        comptime entry_type: entry_mod.EntryType,
        payload: kv_entries.Payload(@backingInt(entry_type)).Value,
    ) !@import("../raft/node.zig").ProposeResult {
        const P = kv_entries.Payload(@backingInt(entry_type));
        comptime std.debug.assert(P.max_size <= MAX_KEY_ENTRY_PAYLOAD);
        const size = P.encodedSize(payload) catch |err| return encodeError(err);
        const buf = shard.scratch.borrow(size);
        defer shard.scratch.release(buf);
        const bytes = P.encodeInto(buf, payload) catch |err| return encodeError(err);
        return try shard.raft_node.propose(entry_type, entry_mod.Flags.NONE, bytes);
    }

    /// For tests: propose a put of `value` at `key` in the default
    /// namespace, as kv.set would.
    pub fn testProposePut(shard: *Shard, key: []const u8, value: []const u8) !@import("../raft/node.zig").ProposeResult {
        return proposeKV(shard, .kv_put, .{ .key = key, .value = value, .ttl_ms = null });
    }

    fn encodeError(err: schema.EncodeError) error{ PayloadTooLarge, TtlTooLarge } {
        return switch (err) {
            error.TooLong, error.NoSpaceLeft => error.PayloadTooLarge,
            error.OutOfRange => error.TtlTooLarge,
        };
    }

    /// Transaction `txn_id`, if it is open and was begun in `namespace`.
    /// Ids are small per-shard counters, so one begun in another namespace
    /// is not found from this one. One whose commit is in the log is closed.
    /// Marks it used, so the idle reaper leaves it.
    fn txnIn(txn_table: anytype, txn_id: u64, namespace: []const u8) ?*txn_mod.TxnState {
        const t = txn_table.get(txn_id) orelse return null;
        if (t.committing) return null;
        if (t.namespace_hash != router.namespaceHash(namespace)) return null;
        t.last_active_ms = @import("stdx").time.monotonicMs();
        return t;
    }

    // ── Sink put ─────────────────────────────────────────────────────────

    /// A sink's write for the shard that owns its key, as an inbox message
    /// carries it: `[ns len u8][ns][ttl? u8][ttl u64][key len u16][key][value]`.
    pub const SinkPut = struct {
        namespace: []const u8,
        qkey: []const u8,
        value: []const u8,
        ttl_ms: ?u64,

        /// The message's bytes, owned by the caller (the receiver frees
        /// them), or null out of memory.
        pub fn encode(self: SinkPut, allocator: Allocator) ?[]u8 {
            const buf = allocator.alloc(u8, 1 + self.namespace.len + 9 + 2 + self.qkey.len + self.value.len) catch return null;
            var off: usize = 0;
            buf[0] = @intCast(self.namespace.len);
            @memcpy(buf[1..][0..self.namespace.len], self.namespace);
            off = 1 + self.namespace.len;
            buf[off] = @intFromBool(self.ttl_ms != null);
            std.mem.writeInt(u64, buf[off + 1 ..][0..8], self.ttl_ms orelse 0, .little);
            off += 9;
            std.mem.writeInt(u16, buf[off..][0..2], @intCast(self.qkey.len), .little);
            off += 2;
            @memcpy(buf[off..][0..self.qkey.len], self.qkey);
            off += self.qkey.len;
            @memcpy(buf[off..][0..self.value.len], self.value);
            return buf;
        }

        pub fn decode(bytes: []const u8) ?SinkPut {
            if (bytes.len < 1) return null;
            const ns_end = 1 + @as(usize, bytes[0]);
            if (bytes.len < ns_end + 11) return null;
            const ttl: ?u64 = if (bytes[ns_end] != 0) std.mem.readInt(u64, bytes[ns_end + 1 ..][0..8], .little) else null;
            const key_len = std.mem.readInt(u16, bytes[ns_end + 9 ..][0..2], .little);
            const key_at = ns_end + 11;
            if (bytes.len < key_at + key_len) return null;
            return .{ .namespace = bytes[1..ns_end], .qkey = bytes[key_at..][0..key_len], .value = bytes[key_at + key_len ..], .ttl_ms = ttl };
        }
    };

    /// The processing KV sink's write of `qkey`, which this shard owns:
    /// proposed as any put is, unanswered. The reason it couldn't be, or
    /// null. The caller counts a refusal as a dropped record.
    pub fn sinkPropose(shard: *Shard, namespace: []const u8, qkey: []const u8, value: []const u8, ttl_ms: ?u64) ?[]const u8 {
        if (isReservedKey(kv_state.Qualified.of(qkey).key)) return "access to reserved key denied";
        shard.namespace_handler.proposeImplicitCreate(namespace, shard);
        _ = proposeKV(shard, .kv_put, .{ .key = qkey, .value = value, .ttl_ms = ttl_ms }) catch |err| return switch (err) {
            error.NotLeader => "this node doesn't lead its group",
            error.WritesStopped => "the shard stopped taking writes",
            error.Halted => "the shard is halted",
            error.Overloaded => "too many writes wait for commit",
            else => @errorName(err),
        };
        shard.namespace_handler.markNamespaceHasData(namespace, shard);
        return null;
    }

    // ── Blocking GET Waiter Management ──────────────────────────────────
    // (Moved to unified WaiterPool in node/waiter_pool.zig)
    // Handlers use shard.waiter_pool.register() / .notify() / .expireTimeouts()
};

// ═══════════════════════════════════════════════════════════════════════════════
// Utilities
// ═══════════════════════════════════════════════════════════════════════════════

/// Check if a key is reserved (system-owned).
fn isReservedKey(key: []const u8) bool {
    for (RESERVED_PREFIXES) |prefix| {
        if (key.len >= prefix.len and std.mem.eql(u8, key[0..prefix.len], prefix)) {
            return true;
        }
    }
    return false;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "kv handler: every KV op in the table has a handler, and the key-routed ones a route" {
    var dispatcher = Dispatcher.init();
    KVHandler.register(&dispatcher);
    inline for (kv_ops.table) |o| {
        try testing.expect(dispatcher.serves(o.code));
        // scan walks every shard; mget routes on its routing key.
        const routed = (o.route == .key and o.fanout == .none) or o.route == .field;
        try testing.expectEqual(routed, dispatcher.pre_route[o.code] != null);
        try testing.expectEqual(o.txn != null, dispatcher.shard_route[o.code] != null);
    }
    try testing.expect(dispatcher.isWalkOp(@backingInt(OpCode.kv_scan)));
    try testing.expectEqual(@as(u16, kv_ops.table.len), dispatcher.handler_count);
}

test "kv handler: reserved key prefixes" {
    try testing.expect(isReservedKey("_action:compute"));
    try testing.expect(isReservedKey("_worker:job1"));
    try testing.expect(isReservedKey("_sys:config"));
    try testing.expect(isReservedKey("_internal:state"));
    try testing.expect(isReservedKey("_flo:metadata"));
    try testing.expect(!isReservedKey("normal_key"));
    try testing.expect(!isReservedKey("_other:prefix")); // not a reserved prefix
    try testing.expect(!isReservedKey("")); // empty key
}

test "kv handler: the key walk refuses a NUL filter and lists only default's keys in default" {
    const allocator = testing.allocator;
    var store = @import("../contract/store.zig").Store.init(allocator);
    defer store.deinit();
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    var kv: KvState = .{ .store = &store };
    var pbuf: [64]u8 = undefined;
    for ([_][2][]const u8{ .{ "b\x00k", "b-value" }, .{ "mine", "default-value" } }, 1..) |kvp, i| {
        const e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, i, i, try kv_entries.Put.encodeInto(&pbuf, .{ .key = kvp[0], .value = kvp[1], .ttl_ms = null }));
        var out: Result = .{};
        out.reset(&e);
        try @import("../contract/machine.zig").applyEntry(&@import("machine.zig").machine, &store, &scratch, &e, &out);
    }

    try testing.expectEqual(@as(usize, 0), KVHandler.localScanKeys(&kv, "default", "b\x00", null, 0).items.len);
    const all = KVHandler.localScanKeys(&kv, "default", "", null, 0).items;
    try testing.expectEqual(@as(usize, 1), all.len);
    try testing.expectEqualStrings("mine", all[0]);
    const b_keys = KVHandler.localScanKeys(&kv, "b", "", null, 0).items;
    try testing.expectEqual(@as(usize, 1), b_keys.len);
    try testing.expectEqualStrings("k", b_keys[0]);
}
