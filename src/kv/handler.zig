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
const walk_mod = @import("../node/walk.zig");
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
const kv_machine = @import("machine.zig");
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

/// The largest single-key KV entry: what `proposeKV` borrows scratch for.
/// A transaction's batch is larger and allocated.
pub const MAX_KEY_ENTRY_PAYLOAD: usize = @max(kv_entries.Put.max_size, kv_entries.JsonPatch.max_size);

/// The longest key a request names, in any namespace.
const MAX_QUALIFIED_KEY = ns_keys.MAX_QUALIFIED_KEY;
/// A key as state keeps it: its namespace's id, then the key.
const MAX_KV_KEY = @import("../protocol/limits.zig").MAX_KV_KEY;
const Key = kv_state.Key;

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
    /// Key-routed ops route by the key's {tag}, or the whole key.
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
            dispatcher.registerWithRoute(o[1], table_run.handler(o[0], KVHandler, o[2]), table_run.routeByKey);
            // A request in a transaction runs on the shard its id names.
            if (table_run.op(o[0]).txn != null) dispatcher.setShardRoute(o[1], txnShardOfBody);
        }
        // Many keys: run where the client is, gathering the rest.
        dispatcher.register(.kv_mget, table_run.handler("kv.mget", KVHandler, mget));
        // Every shard: walked from the client's.
        dispatcher.registerPagedWalk(.kv_scan, table_run.handler("kv.scan", KVHandler, scan), localScanKeys);
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
        NoSuchTransaction,
        KeysNotColocated,
        TxnTooLarge,
        WaitInTxn,
        TooManyWaiters,
        LeaderUnknown,
        WritesStopped,
        TooManyProposals,
        TooLarge,
        AnswerTooLarge,
        PathNotFound,
        BadPath,
        NotADocument,
        NotJson,
        TooDeep,
        TooManyTransactions,
        ScanShort,
        NotOwner,
        GatherBusy,
        NamespaceUnknown,
        Internal,
    };

    pub fn refusal(err: Error) table_run.Refused {
        return switch (err) {
            error.KeyEmpty => refused(.bad_request, .malformed, "bad request: key is required"),
            error.KeyHasNul => refused(.bad_request, .malformed, "bad request: key must not contain NUL"),
            error.KeyTooLong => refused(.bad_request, .field_too_long, std.fmt.comptimePrint("bad request: key is too long: at most {d} bytes", .{MAX_QUALIFIED_KEY})),
            error.ReservedKey => refused(.unauthorized, .reserved_key, "unauthorized: the key is in a prefix the server keeps for itself"),
            error.NotFound => refused(.not_found, .not_found, "not found"),
            error.NoSuchTransaction => refused(.not_found, .no_such_transaction, "not found: no such transaction — it committed, rolled back, or was begun in another namespace"),
            error.KeysNotColocated => refused(.bad_request, .keys_not_colocated, "bad request: this key routes elsewhere than the key the transaction was begun with; give every key in a transaction the same {tag}, e.g. {user:1}:balance"),
            error.TxnTooLarge => refused(.bad_request, .txn_too_large, "bad request: the transaction holds as many ops or bytes as it may"),
            error.WaitInTxn => refused(.bad_request, .conflicting_fields, "bad request: a read in a transaction can't wait: give wait_ms or txn, not both"),
            error.TooManyWaiters => refused(.overloaded, .too_many_waiters, "overloaded: too many reads waiting on this shard — retry"),
            error.LeaderUnknown => refused(.unavailable, .leader_unknown, persistence_mod.failureMessage(error.NotLeader, "")),
            error.WritesStopped => refused(.unavailable, .writes_stopped, persistence_mod.failureMessage(error.WritesStopped, "")),
            error.TooManyProposals => refused(.overloaded, .too_many_proposals, persistence_mod.failureMessage(error.Overloaded, "")),
            error.AnswerTooLarge => refused(.bad_request, .field_too_long, std.fmt.comptimePrint("bad request: the JSON at that path is more than an answer holds: at most {d} bytes", .{kv_ops.MAX_VALUE})),
            error.TooLarge => refused(.bad_request, .field_too_long, kv_machine.TOO_LARGE),
            error.PathNotFound => refused(.not_found, .not_found, kv_machine.PATH_NOT_FOUND),
            error.BadPath => refused(.bad_request, .malformed, kv_machine.BAD_PATH),
            error.NotADocument => refused(.bad_request, .wrong_type, kv_machine.NOT_A_DOCUMENT),
            error.NotJson => refused(.bad_request, .malformed, "bad request: document isn't JSON"),
            error.TooDeep => refused(.bad_request, .field_too_long, kv_machine.TOO_DEEP),
            error.TooManyTransactions => refused(.overloaded, .too_many_transactions, "overloaded: too many open transactions on this shard — commit or roll some back, or retry"),
            error.ScanShort => refused(.overloaded, .shard_busy, "overloaded: no memory to list this page — retry"),
            error.GatherBusy => refused(.overloaded, .shard_busy, "overloaded: too many reads across shards are under way; this request was not run — retry"),
            error.NotOwner => refused(.internal_error, .internal, "internal error: a shard was asked for a key another shard owns"),
            error.NamespaceUnknown => refused(.unavailable, .unclassified, persistence_mod.failureMessage(error.NamespaceUnknown, "")),
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
        txn: ?u64,
    ) Error!table_run.Outcome(GetResponse) {
        const in_txn = try txnOf(shard, req, txn);
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        const key = try tableKey(&kbuf, req);
        const kv = shard.kv_handler.*.kv;
        if (in_txn) |t| {
            if (wait_ms > 0) return error.WaitInTxn;
            if (shard.kv_handler.*.txn_table.lastOpForKey(t.txn_id, key.raw)) |last| switch (last.kind) {
                .delete => return error.NotFound,
                // Not committed, so no version or expiry yet.
                .put => return .{ .answer = .{ .version = 0, .value = last.value, .expires_at_ms = null } },
                // Their value isn't the key's: read what's committed.
                .expire, .incr => {},
            };
        }
        const current = kv.get(key.stored);
        if (wait_ms > 0) {
            if (wait_for == .exists) if (current) |e| return .{ .answer = getAnswer(e) };
            const registered = shard.waiter_pool.register(.{
                .kind = .kv_get,
                .reply_to = conn.replyTo(),
                .request_id = req.header.request_id,
                .key = key.stored,
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
        txn: ?u64,
    ) Error!table_run.Outcome(SetResponse) {
        const in_txn = try txnOf(shard, req, txn);
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        const key = try tableKey(&kbuf, req);
        const ttl = ttl_ms orelse 0;
        if (in_txn) |t| {
            try bufferInTxn(shard, t, key, .put, value, ttl, condition, cas_version);
            return .{ .answer = .{ .version = null } };
        }
        const proposed = proposeKV(shard, .kv_put, .{ .key = key.stored, .value = value, .ttl_ms = if (ttl == 0) null else ttl, .condition = condition, .cas_version = cas_version }) catch |err| return proposeError(err);
        shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        return .{ .park = .{ .proposed = proposed, .answer = answerSet } };
    }

    /// The set applied: answered with the version its applier gave the key.
    fn answerSet(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        table_run.answer(SetResponse, shard, conn, request_id, .{ .version = r.int(u64) orelse 0 });
    }

    /// The request's key in its namespace, once it passes the checks every
    /// key does; its stored form is written to `buf`.
    fn tableKey(buf: *[MAX_KV_KEY]u8, req: Request) Error!Key {
        try checkKey(req.key);
        return .init(buf, req.ns_id, req.key);
    }

    /// Why `raw` can't be a key a client reads or writes, if it can't. A
    /// key with a NUL byte is refused though the stored key, which leads
    /// with a fixed-width namespace id, could hold one: the CLI and SDKs
    /// have always refused it, so no client has one to read back.
    fn checkKey(raw: []const u8) Error!void {
        if (raw.len == 0) return error.KeyEmpty;
        if (isReservedKey(raw)) return error.ReservedKey;
        if (std.mem.indexOfScalar(u8, raw, 0) != null) return error.KeyHasNul;
        if (raw.len > MAX_QUALIFIED_KEY) return error.KeyTooLong;
    }

    /// Transaction `id`, open in the request's namespace, if the request's
    /// key routes with the key it was begun with. The routing hash is
    /// compared, not the shard: keys that share a shard today needn't later.
    fn openTxn(shard: *Shard, req: Request, id: u64) Error!*txn_mod.TxnState {
        const t = txnIn(&shard.kv_handler.*.txn_table, id, req.ns_id) orelse return error.NoSuchTransaction;
        if (t.pinned_hash != router.routeNsKey(req.ns_id, req.key)) return error.KeysNotColocated;
        return t;
    }

    /// The transaction a request names, if any. Looked up before any other
    /// check: a request refused for its key still uses the
    /// transaction, and one that is gone is answered as gone.
    fn txnOf(shard: *Shard, req: Request, txn: ?u64) Error!?*txn_mod.TxnState {
        return if (txn) |id| try openTxn(shard, req, id) else null;
    }

    fn proposeError(err: anyerror) Error {
        return switch (err) {
            error.NotLeader => error.LeaderUnknown,
            error.WritesStopped => error.WritesStopped,
            error.Overloaded => error.TooManyProposals,
            error.PayloadTooLarge => error.TooLarge,
            error.NamespaceUnknown => error.NamespaceUnknown,
            else => error.Internal,
        };
    }

    /// The shard a table op's transaction lives on, when it names one.
    fn txnShardOfBody(req: Request) ?u16 {
        return txn_mod.idShard(table_run.txnId(req) orelse return null);
    }

    pub const DeleteResponse = table_run.op("kv.delete").response;
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
        txn: ?u64,
    ) Error!table_run.Outcome(DeleteResponse) {
        const in_txn = try txnOf(shard, req, txn);
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        const key = try tableKey(&kbuf, req);
        if (in_txn) |t| {
            try bufferInTxn(shard, t, key, .delete, "", 0, .always, cas_version);
            return .{ .answer = .{ .existed = null } };
        }
        const proposed = proposeKV(shard, .kv_delete, .{ .key = key.stored, .cas_version = cas_version }) catch |err| return proposeError(err);
        return .{ .park = .{ .proposed = proposed, .answer = answerDeleted } };
    }

    /// Whether the key was there when the delete applied: another delete
    /// can apply first.
    fn answerDeleted(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        table_run.answer(DeleteResponse, shard, conn, request_id, .{ .existed = (r.int(u8) orelse 0) == 1 });
    }

    /// The names of keys starting with the request's key, a page at a time
    /// across every shard: each shard answers its page from its own keys
    /// (`walk.start`), and this one merges them.
    fn scan(
        shard: *Shard,
        conn: *Connection,
        req: Request,
        limit: u32,
        cursor: []const u8,
    ) Error!table_run.Outcome(ScanResponse) {
        const NameWalker = dispatcher_mod.NameWalker;
        const self = shard.kv_handler;
        const from = if (cursor.len == 0) null else cursor;
        const Names = ScanResponse.field_list[0].T;
        if (shard.shardCount() > 1) return switch (walk_mod.start(shard, conn, req, @intCast(shard.shardCount()), limit, from)) {
            .asked => .wait,
            .past_end => .{ .answer = .{ .names = Names.Builder.init(&self.answer_buf).value(), .cursor = null } },
            .refused => |r| .{ .refused = r },
        };
        const contexts = [_]*anyopaque{@ptrCast(self.kv)};
        const walker = NameWalker.initPaged(localScanKeys, 1);
        var found: [NameWalker.MAX_BATCH][]const u8 = undefined;
        var want: u32 = @min(limit, NameWalker.MAX_BATCH);
        while (true) {
            _ = dispatcher_mod.takeScanShort();
            const page = walker.walk(&contexts, .{ .name = req.namespace, .id = req.ns_id }, req.key, from, want, &found, &self.cursor_buf);
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
    fn history(shard: *Shard, _: *Connection, req: Request, limit: u32, before_version: ?u64) Error!table_run.Outcome(HistoryResponse) {
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        const key = try tableKey(&kbuf, req);
        const self = shard.kv_handler;
        var it = self.kv.history(key, before_version);
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
    fn mget(shard: *Shard, conn: *Connection, req: Request, keys: MgetKeys.Value) Error!table_run.Outcome(MgetResponse) {
        // Another shard asked this one for the keys it owns: a key that
        // routes elsewhere is refused, never read here or answered missing.
        const forwarded = conn.proxy_for != null;
        // Keys other shards own are read on those shards: each owner gets
        // its part, and the answers are merged here once all have come.
        if (!forwarded and shard.shardCount() > 1) {
            var plan = mget_mod.plan(shard.allocator, keys, MgetOwner{ .shard = shard, .ns_id = req.ns_id }, MgetOwner.of) catch return error.Internal;
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
            const found = try mgetLookup(shard, req.ns_id, raw_key);
            if (!(entries.add(.{ .found = found.found, .version = found.version, .value = found.value }) catch return error.Internal)) {
                return .{ .answer = .{ .entries = entries.value(), .next = index } };
            }
        }
        return .{ .answer = .{ .entries = entries.value(), .next = null } };
    }

    /// One key of an mget, from this shard's own map. A key that can't be
    /// one, or one the server keeps, is not found. The key must be this
    /// shard's: one another shard owns is never read here or answered
    /// missing.
    fn mgetLookup(shard: *Shard, ns_id: router.NamespaceId, raw_key: []const u8) Error!MgetLookup {
        const none: MgetLookup = .{ .found = false, .value = "", .version = 0 };
        checkKey(raw_key) catch return none;
        const owner = MgetOwner.of(.{ .shard = shard, .ns_id = ns_id }, raw_key);
        if (owner != shard.id) {
            log.err("shard {d}: kv.mget was asked for key \"{s}\", which shard {d} owns", .{ shard.id, raw_key, owner });
            return error.NotOwner;
        }
        const entry = shard.kv_handler.*.kv.getIn(ns_id, raw_key) orelse return none;
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
                local = try localPart(shard, req.ns_id, part_keys);
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
    fn localPart(shard: *Shard, ns_id: router.NamespaceId, keys: []const []const u8) Error![]u8 {
        const Entries = MgetResponse.field_list[0].T;
        var entries = Entries.Builder.init(&shard.kv_handler.answer_buf);
        var next: ?u16 = null;
        for (keys, 0..) |raw_key, i| {
            const found = try mgetLookup(shard, ns_id, raw_key);
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
        ns_id: router.NamespaceId,

        fn of(self: MgetOwner, raw_key: []const u8) u16 {
            checkKey(raw_key) catch return self.shard.id;
            const r = &self.shard.router;
            return r.partitionToShard(r.hashToPartition(router.routeNsKey(self.ns_id, raw_key)));
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
        txn: ?u64,
    ) Error!table_run.Outcome(IncrResponse) {
        const in_txn = try txnOf(shard, req, txn);
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        const key = try tableKey(&kbuf, req);
        var delta_bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &delta_bytes, delta, .little);
        if (in_txn) |t| {
            try bufferInTxn(shard, t, key, .incr, &delta_bytes, 0, .always, null);
            return .{ .answer = .{ .version = null, .value = null } };
        }
        const proposed = proposeKV(shard, .kv_incr, .{ .key = key.stored, .delta = delta }) catch |err| return proposeError(err);
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
        txn: ?u64,
    ) Error!table_run.Outcome(ExpireResponse) {
        const in_txn = try txnOf(shard, req, txn);
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        const key = try tableKey(&kbuf, req);
        if (in_txn) |t| {
            try bufferInTxn(shard, t, key, .expire, "", ttl_ms, .always, cas_version);
            return .{ .answer = .{} };
        }
        const proposed = proposeKV(shard, .kv_expire, .{ .key = key.stored, .ttl_ms = ttl_ms, .cas_version = cas_version }) catch |err| return proposeError(err);
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
        txn: ?u64,
    ) Error!table_run.Outcome(ExistsResponse) {
        const in_txn = try txnOf(shard, req, txn);
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        const key = try tableKey(&kbuf, req);
        if (in_txn) |t| {
            if (shard.kv_handler.*.txn_table.lastOpForKey(t.txn_id, key.raw)) |last| switch (last.kind) {
                .delete => return .{ .answer = .{ .exists = false, .version = 0 } },
                .put => return .{ .answer = .{ .exists = true, .version = 0 } },
                .expire, .incr => {},
            };
        }
        const e = shard.kv_handler.*.kv.get(key.stored) orelse return .{ .answer = .{ .exists = false, .version = 0 } };
        return .{ .answer = .{ .exists = true, .version = e.version } };
    }

    /// The JSON at `path` in a key's document, with the document's version.
    fn jsonGet(
        shard: *Shard,
        _: *Connection,
        req: Request,
        path: []const u8,
    ) Error!table_run.Outcome(JsonGetResponse) {
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        const key = try tableKey(&kbuf, req);
        const self = shard.kv_handler;
        const e = self.kv.get(key.stored) orelse return error.NotFound;
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
    ) Error!table_run.Outcome(JsonSetResponse) {
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        const key = try tableKey(&kbuf, req);
        const self = shard.kv_handler;
        if (!json_depth.within(document)) return error.TooDeep;
        if (!(std.json.validate(self.allocator, document) catch return error.Internal)) return error.NotJson;
        // Merged with the key's value at apply, not here: a write between
        // this read and the apply would otherwise be lost.
        const proposed = proposeKV(shard, .kv_json_patch, .{ .key = key.stored, .path = path, .document = document }) catch |err| return proposeError(err);
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
    ) Error!table_run.Outcome(JsonDelResponse) {
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        const key = try tableKey(&kbuf, req);
        const proposed = proposeKV(shard, .kv_json_patch, .{ .key = key.stored, .path = path, .document = null }) catch |err| return proposeError(err);
        return .{ .park = .{ .proposed = proposed, .answer = answerJsonDel } };
    }

    /// The path was deleted: the document's version now, or none when `$`
    /// deleted the key.
    fn answerJsonDel(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        const version = r.int(u64) orelse 0;
        table_run.answer(JsonDelResponse, shard, conn, request_id, .{ .version = if (version == 0) null else version });
    }

    pub fn jsonError(err: json_path.PathError) Error {
        return switch (err) {
            error.PathNotFound, error.NotAnObject, error.NotAnArray, error.IndexOutOfBounds => error.PathNotFound,
            error.InvalidPath => error.BadPath,
            error.InvalidJson => error.NotADocument,
            error.TooDeep => error.TooDeep,
            error.OutOfMemory => error.Internal,
        };
    }

    /// Begin a transaction pinned to the routing hash of the request's key:
    /// its {tag}, or the whole key when it has none. Every key in the
    /// transaction must route with it.
    fn txnBegin(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(TxnBeginResponse) {
        if (req.key.len == 0) return error.KeyEmpty;
        const pinned = router.routeNsKey(req.ns_id, req.key);
        const id = shard.kv_handler.*.txn_table.begin(pinned, req.ns_id) catch |err| return switch (err) {
            error.TooManyOpenTxns => error.TooManyTransactions,
            error.OutOfMemory => error.Internal,
        };
        return .{ .answer = .{ .txn = id, .pinned = pinned } };
    }

    /// Commit a transaction's writes as one log entry.
    fn txnCommit(shard: *Shard, _: *Connection, req: Request, txn: u64) Error!table_run.Outcome(TxnCommitResponse) {
        const self = shard.kv_handler;
        const t = txnIn(&self.txn_table, txn, req.ns_id) orelse return error.NoSuchTransaction;
        if (t.ops.items.len == 0) {
            self.txn_table.drop(txn);
            return .{ .answer = .{ .commit_index = shard.raft_node.commit_index, .ops = 0 } };
        }
        // Begun in a namespace this shard knew; a write here never names id 0.
        if (t.ns_id == .invalid) return error.NamespaceUnknown;
        const payload = txn_mod.encodeBatch(self.allocator, t.ns_id, t.ops.items) catch return error.Internal;
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
        _ = txnIn(table, txn, req.ns_id) orelse return error.NoSuchTransaction;
        table.drop(txn);
        return .{ .answer = .{} };
    }

    /// Buffer a write in transaction `t`.
    fn bufferInTxn(
        shard: *Shard,
        t: *txn_mod.TxnState,
        key: Key,
        kind: txn_mod.TxnOpKind,
        value: []const u8,
        ttl_ms: u64,
        condition: kv_ops.Condition,
        cas_version: ?u64,
    ) Error!void {
        // The transaction is in the key's namespace (`txnIn`): its ops keep
        // the key as named, and its batch names the namespace once.
        std.debug.assert(key.ns == t.ns_id);
        shard.kv_handler.*.txn_table.appendOp(t.txn_id, kind, key.raw, value, ttl_ms, condition, cas_version) catch |err| return switch (err) {
            error.TxnTooLarge => error.TxnTooLarge,
            error.OutOfMemory => error.Internal,
        };
    }

    // ── Shard Walker: Local Scan ──────────────────────────────────────

    /// ShardWalker LocalScanFn for kv_scan — scans key names from one shard's
    /// KV state. Returns borrowed references (zero allocation).
    /// Walks the namespace's id prefix only, filters reserved keys, and
    /// returns keys as the client named them.
    /// When filter is non-empty, only returns keys matching the prefix filter.
    /// Pages by key: a page starts after `local_cursor`, the last key the
    /// one before it returned, so a key written or deleted between pages
    /// moves no other key in or out of the walk.
    fn localScanKeys(
        ctx: *anyopaque,
        walk_ns: dispatcher_mod.WalkNamespace,
        filter: []const u8,
        local_cursor: ?[]const u8,
        limit: u32,
    ) dispatcher_mod.NameWalker.ScanResult {
        const kv: *KvState = @ptrCast(@alignCast(ctx));
        // No key holds a NUL (`checkKey`), so a filter that does names nothing.
        if (std.mem.indexOfScalar(u8, filter, 0) != null) return .{ .items = &.{}, .next_cursor = null };
        // Keys come back in key order and without their namespace.
        const ns = walk_ns.id;
        // One past the page shows whether more follow.
        const cap: usize = @min(limit, dispatcher_mod.NameWalker.MAX_BATCH);
        if (cap == 0) return .{ .items = &.{}, .next_cursor = local_cursor };
        const key_buf = dispatcher_mod.scanScratch(cap + 1);
        var after = local_cursor;
        var count: usize = 0;
        while (count < key_buf.len) {
            const raw = kv.scanKeyNames(ns, filter, after, key_buf[count..]);
            if (raw == 0) break;
            // The scan resumes after the last key it read, reserved or not.
            after = key_buf[count + raw - 1];
            const asked = key_buf.len - count;
            for (key_buf[count .. count + raw]) |key| {
                if (!isReservedKey(key)) {
                    key_buf[count] = key;
                    count += 1;
                }
            }
            if (raw < asked) break;
        }
        if (count > cap) return .{ .items = key_buf[0..cap], .next_cursor = key_buf[cap - 1] };
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
        // A write runs once its namespace is known here (`prepareWrite`); a
        // key naming id 0 would halt every replica that applied it.
        if (Key.ofStored(payload.key) == null) return error.NamespaceUnknown;
        const size = P.encodedSize(payload) catch |err| return encodeError(err);
        const buf = shard.scratch.borrow(size);
        defer shard.scratch.release(buf);
        const bytes = P.encodeInto(buf, payload) catch |err| return encodeError(err);
        return try shard.raft_node.propose(entry_type, entry_mod.Flags.NONE, bytes);
    }

    /// For tests: propose a put of `value` at `key` in the default
    /// namespace, as kv.set would.
    pub fn testProposePut(shard: *Shard, key: []const u8, value: []const u8) !@import("../raft/node.zig").ProposeResult {
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        return proposeKV(shard, .kv_put, .{ .key = Key.init(&kbuf, .default, key).stored, .value = value, .ttl_ms = null, .condition = .always, .cas_version = null });
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
    fn txnIn(txn_table: anytype, txn_id: u64, ns_id: router.NamespaceId) ?*txn_mod.TxnState {
        const t = txn_table.get(txn_id) orelse return null;
        if (t.committing) return null;
        if (t.ns_id != ns_id) return null;
        t.last_active_ms = @import("stdx").time.monotonicMs();
        return t;
    }

    // ── Sink put ─────────────────────────────────────────────────────────

    /// A sink's write for the shard that owns its key, as an inbox message
    /// carries it: `[ns len u8][ns][ttl? u8][ttl u64][key len u16][key][value]`.
    /// The key is as the sink named it; the owner finds the namespace's id.
    pub const SinkPut = struct {
        namespace: []const u8,
        key: []const u8,
        value: []const u8,
        ttl_ms: ?u64,

        /// The message's bytes, owned by the caller (the receiver frees
        /// them), or null out of memory.
        pub fn encode(self: SinkPut, allocator: Allocator) ?[]u8 {
            const buf = allocator.alloc(u8, 1 + self.namespace.len + 9 + 2 + self.key.len + self.value.len) catch return null;
            var off: usize = 0;
            buf[0] = @intCast(self.namespace.len);
            @memcpy(buf[1..][0..self.namespace.len], self.namespace);
            off = 1 + self.namespace.len;
            buf[off] = @intFromBool(self.ttl_ms != null);
            std.mem.writeInt(u64, buf[off + 1 ..][0..8], self.ttl_ms orelse 0, .little);
            off += 9;
            std.mem.writeInt(u16, buf[off..][0..2], @intCast(self.key.len), .little);
            off += 2;
            @memcpy(buf[off..][0..self.key.len], self.key);
            off += self.key.len;
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
            return .{ .namespace = bytes[1..ns_end], .key = bytes[key_at..][0..key_len], .value = bytes[key_at + key_len ..], .ttl_ms = ttl };
        }
    };

    /// The processing KV sink's write of `raw_key` in `namespace`, which
    /// this shard owns: proposed as any put is, unanswered. The reason it
    /// couldn't be, or null. The caller counts a refusal as a dropped record.
    pub fn sinkPropose(shard: *Shard, namespace: []const u8, raw_key: []const u8, value: []const u8, ttl_ms: ?u64) ?[]const u8 {
        checkKey(raw_key) catch |err| return refusal(err).refusal.message;
        shard.namespace_handler.proposeImplicitCreate(namespace, shard);
        const ns_id = shard.namespaceId(namespace);
        if (ns_id == .invalid) return "the namespace isn't created on this shard yet";
        var kbuf: [MAX_KV_KEY]u8 = undefined;
        _ = proposeKV(shard, .kv_put, .{ .key = Key.init(&kbuf, ns_id, raw_key).stored, .value = value, .ttl_ms = ttl_ms, .condition = .always, .cas_version = null }) catch |err| return switch (err) {
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
        // scan walks every shard; mget runs where it lands.
        const routed = o.route == .key and o.fanout == .none;
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

test "kv handler: the key walk refuses a NUL filter and lists only the namespace's own keys" {
    const allocator = testing.allocator;
    var store = @import("../contract/store.zig").Store.init(allocator);
    defer store.deinit();
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    var kv: KvState = .{ .store = &store };
    var pbuf: [64]u8 = undefined;
    var kbuf: [MAX_KV_KEY]u8 = undefined;
    const other: router.NamespaceId = .of(16);
    for ([_]struct { router.NamespaceId, []const u8, []const u8 }{ .{ other, "k", "b-value" }, .{ .default, "mine", "default-value" } }, 1..) |kvp, i| {
        const e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, i, i, try kv_entries.Put.encodeInto(&pbuf, .{ .key = Key.init(&kbuf, kvp[0], kvp[1]).stored, .value = kvp[2], .ttl_ms = null, .condition = .always, .cas_version = null }));
        var out: Result = .{};
        out.reset(&e);
        try @import("../contract/machine.zig").applyEntry(&@import("machine.zig").machine, &store, &scratch, &e, &out);
    }

    try testing.expectEqual(@as(usize, 0), KVHandler.localScanKeys(&kv, .{ .name = "default", .id = .default }, "b\x00", null, 10).items.len);
    const all = KVHandler.localScanKeys(&kv, .{ .name = "default", .id = .default }, "", null, 10).items;
    try testing.expectEqual(@as(usize, 1), all.len);
    try testing.expectEqualStrings("mine", all[0]);
    const b_keys = KVHandler.localScanKeys(&kv, .{ .name = "b", .id = other }, "", null, 10).items;
    try testing.expectEqual(@as(usize, 1), b_keys.len);
    try testing.expectEqualStrings("k", b_keys[0]);
    // A namespace no one created holds no keys.
    try testing.expectEqual(@as(usize, 0), KVHandler.localScanKeys(&kv, .{ .name = "nope" }, "", null, 10).items.len);
}
