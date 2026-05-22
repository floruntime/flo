//! KV Handler — registers KV opcodes with Dispatcher and handles KV operations.
//!
//! Read operations (get, scan, history) query the KV projection directly.
//! Write operations (put, delete) go through the full Raft propose pipeline:
//!   1. Build CommandPayload from the request (key + value + TTL)
//!   2. Propose the entry to RaftNode — in single-node mode this commits immediately
//!   3. Apply all newly committed entries (commit_index > last_applied) to the
//!      KV projection via applyEntry()
//!   4. Send the response to the client
//!
//! In multi-node mode the leader replicates via AppendEntries before committing.
//! The shard's tick loop drives replication; this handler waits for commit by
//! calling applyCommittedEntries() which reads up to commit_index.
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
//!   → [writes] raft_node.propose() → applyCommittedEntries() → sendResponse
//!   → [reads]  projection.get/scan() → sendResponse
//!
//! ## Reserved Keys
//!
//! Keys prefixed with `_action:`, `_worker:`, `_sys:`, `_internal:`, `_flo:`
//! are system-owned and blocked from user operations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const kv_mod = @import("../projection/kv.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const waiter_pool_mod = @import("../node/waiter_pool.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const network_mode = @import("../raft/network.zig");
const ns_keys = @import("../namespace/handler.zig");
const router = @import("../node/router.zig");
const txn_mod = @import("./txn.zig");
const log = @import("stdx").log;
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;

const CommandResult = result_mod.CommandResult;
const KVProjection = kv_mod.KVProjection;
const ScanEntry = kv_mod.ScanEntry;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const OptionsBuilder = proto.OptionsBuilder;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const RaftNetwork = network_mode.RaftNetwork;

/// Max serialized payload for a UAL entry (key + value + command prefix + TTL).
const MAX_ENTRY_PAYLOAD = 256 * 1024 + 64;

/// Re-export from centralized namespace module for local convenience.
const MAX_QUALIFIED_KEY = ns_keys.MAX_QUALIFIED_KEY;
const qualifyKey = ns_keys.qualifyKey;
const validateKeySize = ns_keys.validateKeySize;
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

/// Maximum number of scan results in a single response.
const MAX_SCAN_LIMIT: u32 = 1000;

/// Default scan limit when no limit option is provided.
const DEFAULT_SCAN_LIMIT: u32 = 100;

// ═══════════════════════════════════════════════════════════════════════════════
// KVHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const KVHandler = struct {
    // ── Fields ──────────────────────────────────────────────────────────

    kv: *KVProjection,
    allocator: Allocator,

    /// Counter for direct-write path (handleCommand used in tests and internal ops).
    /// Production writes use the Raft log index as their version.
    next_lsn: u64,

    /// Global metrics registry (optional, set by runtime when dashboard is enabled).
    metrics_registry: ?*MetricsRegistry,

    /// Per-shard transaction table. Lives next to the projection — same
    /// thread, no locks. Tracks open BEGIN'd txns until COMMIT/ROLLBACK.
    txn_table: txn_mod.TxnTable,

    pub fn init(allocator: Allocator, kv: *KVProjection) KVHandler {
        return .{
            .kv = kv,
            .allocator = allocator,
            .next_lsn = 1,
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
        dispatcher.registerWithRoute(.kv_get, dispatchGet, preRouteByKey);
        dispatcher.registerWithRoute(.kv_put, dispatchPut, preRouteByKey);
        dispatcher.registerWithRoute(.kv_delete, dispatchDelete, preRouteByKey);
        dispatcher.registerWalk(.kv_scan, dispatchScan, localScanKeys);
        dispatcher.register(.kv_history, dispatchHistory);
        dispatcher.register(.kv_mget, dispatchMget);

        // Extended KV operations.
        dispatcher.registerWithRoute(.kv_incr, dispatchIncr, preRouteByKey);
        dispatcher.registerWithRoute(.kv_touch, dispatchTouch, preRouteByKey);
        dispatcher.registerWithRoute(.kv_persist, dispatchPersist, preRouteByKey);
        dispatcher.registerWithRoute(.kv_exists, dispatchExists, preRouteByKey);
        dispatcher.registerWithRoute(.kv_json_get, dispatchJsonGet, preRouteByKey);
        dispatcher.registerWithRoute(.kv_json_set, dispatchJsonSet, preRouteByKey);
        dispatcher.registerWithRoute(.kv_json_del, dispatchJsonDel, preRouteByKey);

        // Per-shard transactions. BEGIN pins the txn to the partition that
        // owns the routing key; COMMIT/ROLLBACK then route to the same shard
        // by passing the routing key (the CLI carries it forward).
        dispatcher.registerWithRoute(.kv_begin_txn, dispatchBeginTxn, preRouteByKey);
        dispatcher.registerWithRoute(.kv_commit_txn, dispatchCommitTxn, preRouteByKey);
        dispatcher.registerWithRoute(.kv_rollback_txn, dispatchRollbackTxn, preRouteByKey);
    }

    // ── Pre-Route Hooks ─────────────────────────────────────────────────

    /// Route by key hash — single-partition operations.
    /// Uses namespace-qualified hash: hash(namespace \0 routing_key)
    fn preRouteByKey(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        // Check for explicit routing key option
        if (req.findOption(.routing_key)) |opt| {
            return router.hashKeyWithNamespace(req.namespace, opt.asString());
        }
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    // ── Dispatch Wrappers ───────────────────────────────────────────────
    // These bridge from Dispatcher's (shard, conn, req) signature.
    // When Shard owns Partitions, these will extract the correct
    // Partition's KVProjection based on routing.

    fn dispatchGet(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        // Upfront key size validation — give the user a clear error before any business logic
        if (validateKeySize(req.namespace, req.key)) |err_msg| {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = err_msg } });
            return;
        }

        // Namespace-qualify the key for projection lookup
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } });
            return;
        };

        // Check for blocking options before normal GET.
        // Wire mapping (from CLI client):
        //   CLI --wait  → wire block_ms  (wait-until-exists)
        //   CLI --block → wire wait_ms   (watch-for-changes / next version)
        const block_ms = req.getBlockMs(); // wait-until-exists
        const wait_ms = req.getWaitMs(); // watch-for-changes

        if (block_ms) |bms| {
            // Wait-until-exists semantics: if key exists, return immediately.
            if (shard.kv_handler.*.kv.get(qkey)) |entry| {
                const result = CommandResult{ .kv_value = .{ .value = entry.value, .version = entry.version } };
                sendKVResponse(shard, conn, req.header.request_id, result);
                return;
            }
            // Key absent — register waiter in unified pool; response sent when key is created.
            _ = shard.waiter_pool.register(.{
                .kind = .kv_get,
                .fd = conn.fd,
                .owner_shard = conn.owner_shard,
                .request_id = req.header.request_id,
                .key = qkey,
                .min_version = 0,
                .timeout_ms = bms,
            });
            conn.response_deferred = true;
            return;
        }

        if (wait_ms) |wms| {
            // Watch-for-changes semantics: wait for version > current.
            const current_version: u64 = if (shard.kv_handler.*.kv.get(qkey)) |entry| entry.version else 0;
            _ = shard.waiter_pool.register(.{
                .kind = .kv_get,
                .fd = conn.fd,
                .owner_shard = conn.owner_shard,
                .request_id = req.header.request_id,
                .key = qkey,
                .min_version = current_version,
                .timeout_ms = wms,
            });
            conn.response_deferred = true;
            return;
        }

        // Normal non-blocking GET.
        // Inside a transaction? Check buffered ops first (read-your-writes).
        switch (tryReadInTxn(shard, conn, req, qkey)) {
            .none => {},
            .err => return,
            .deleted => {
                sendKVResponse(shard, conn, req.header.request_id, .kv_not_found);
                return;
            },
            .value => |v| {
                sendKVResponse(shard, conn, req.header.request_id, .{ .kv_value = .{ .value = v, .version = 0 } });
                return;
            },
            .miss => {}, // fall through to projection lookup
        }
        const cmd_result = shard.kv_handler.*.handleCommand(req);
        defer shard.kv_handler.*.freeResult(cmd_result);
        log.debug("KV GET: key={s}, hit={}", .{ req.key, cmd_result != .kv_not_found });
        sendKVResponse(shard, conn, req.header.request_id, cmd_result);
    }

    fn dispatchPut(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        // Upfront key size validation — give the user a clear error before any business logic
        if (validateKeySize(req.namespace, req.key)) |err_msg| {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = err_msg } });
            return;
        }

        // Namespace-qualify the key
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } });
            return;
        };

        // Validate the request before proposing (uses qualified key)
        const validation = shard.kv_handler.*.validatePutQ(req, qkey);
        if (validation) |err_result| {
            defer shard.kv_handler.*.freeResult(err_result);
            sendKVResponse(shard, conn, req.header.request_id, err_result);
            return;
        }

        // Compute absolute expiry_ns for any TTL the request carries.
        var put_expiry_ns: u64 = 0;
        if (req.getTtlSeconds()) |ttl_secs| {
            if (ttl_secs > 0) {
                const now_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
                put_expiry_ns = now_ns + ttl_secs * 1_000_000_000;
            }
        }

        // Inside a transaction? Buffer instead of proposing.
        if (tryHandleInTxn(shard, conn, req, qkey, .put, req.value, put_expiry_ns)) return;

        // Build CommandPayload and propose through Raft (uses qualified key)
        _ = proposeKVEntry(shard, .kv_put, req, qkey) catch |err| {
            const result: CommandResult = switch (err) {
                error.NotLeader => .{ .err = .{ .code = .unavailable, .message = "not leader" } },
                else => .{ .err = .{ .code = .internal_error, .message = "propose failed" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, result);
            return;
        };

        // Apply all committed entries (in single-node mode this is synchronous)
        applyCommittedEntries(shard);

        // Notify any blocking GET waiters for this key via unified pool (qualified key)
        shard.waiter_pool.notify(.kv_get, qkey, @import("../node/shard.zig").resolveKVWaiter, @ptrCast(shard));

        // Build response from the committed version
        const version = if (shard.kv_handler.*.kv.get(qkey)) |entry| entry.version else 1;
        const cmd_result = CommandResult{ .kv_put_ok = .{ .version = version } };
        log.debug("KV PUT: key={s}, value_len={d}, version={d}", .{ req.key, req.value.len, version });

        // Track namespace data for non-empty delete check
        shard.namespace_handler.markNamespaceHasData(req.namespace, shard);

        // Register KV namespace in global metrics registry for dashboard/Prometheus
        if (shard.kv_handler.metrics_registry) |mr| {
            _ = mr.registerKVNamespace(req.namespace) catch {};
        }

        sendKVResponse(shard, conn, req.header.request_id, cmd_result);
    }

    fn dispatchDelete(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        // Upfront key size validation — give the user a clear error before any business logic
        if (validateKeySize(req.namespace, req.key)) |err_msg| {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = err_msg } });
            return;
        }

        // Namespace-qualify the key
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } });
            return;
        };

        // Validate the request
        const validation = shard.kv_handler.*.validateDelete(req);
        if (validation) |err_result| {
            defer shard.kv_handler.*.freeResult(err_result);
            sendKVResponse(shard, conn, req.header.request_id, err_result);
            return;
        }

        // Check the key exists before proposing a delete (qualified key)
        const existing_for_delete = shard.kv_handler.*.kv.get(qkey);
        if (existing_for_delete == null) {
            sendKVResponse(shard, conn, req.header.request_id, .kv_not_found);
            return;
        }

        // CAS check — only the holder of the expected version may delete.
        if (req.getCasVersion()) |expected_version| {
            if (existing_for_delete.?.version != expected_version) {
                sendKVResponse(shard, conn, req.header.request_id, .{ .kv_cas_failed = .{ .current_version = existing_for_delete.?.version } });
                return;
            }
        }

        // Inside a transaction? Buffer the delete.
        if (tryHandleInTxn(shard, conn, req, qkey, .delete, &[_]u8{}, 0)) return;

        // Propose the delete through Raft (qualified key)
        _ = proposeKVEntry(shard, .kv_delete, req, qkey) catch |err| {
            const result: CommandResult = switch (err) {
                error.NotLeader => .{ .err = .{ .code = .unavailable, .message = "not leader" } },
                else => .{ .err = .{ .code = .internal_error, .message = "propose failed" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, result);
            return;
        };

        // Apply all committed entries
        applyCommittedEntries(shard);

        // Notify any blocking GET waiters for this key via unified pool (qualified key)
        shard.waiter_pool.notify(.kv_get, qkey, @import("../node/shard.zig").resolveKVWaiter, @ptrCast(shard));

        log.debug("KV DELETE: key={s}", .{req.key});
        sendKVResponse(shard, conn, req.header.request_id, .ok);
    }

    // ── Extended KV: INCR / TOUCH / PERSIST / EXISTS / JSON ────────────

    /// INCR — atomic counter. Wire format:
    ///   value field = 8-byte i64 little-endian delta (default 1 if empty).
    /// Response: kv_value with value = 8-byte i64 LE new counter.
    fn dispatchIncr(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        if (validateKeySize(req.namespace, req.key)) |err_msg| {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = err_msg } });
            return;
        }
        if (req.key.len == 0) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "key is required" } });
            return;
        }
        if (isReservedKey(req.key)) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } });
            return;
        }

        // Parse delta — empty value defaults to +1.
        var delta: i64 = 1;
        if (req.value.len == 8) {
            delta = std.mem.readInt(i64, req.value[0..8], .little);
        } else if (req.value.len != 0) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "incr: value must be empty or 8-byte i64 LE" } });
            return;
        }

        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } });
            return;
        };

        // Reject INCR against an existing non-counter value (string of != 8 bytes).
        if (shard.kv_handler.*.kv.getRaw(qkey)) |existing| {
            if (!existing.tombstone and existing.value.len != 8) {
                sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "value is not a counter" } });
                return;
            }
        }

        // Encode delta in the entry value field.
        var val_buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &val_buf, delta, .little);

        // Inside a transaction? Buffer the increment.
        if (tryHandleInTxn(shard, conn, req, qkey, .incr, &val_buf, 0)) return;

        _ = proposeKVEntryWithValue(shard, .kv_incr, req, qkey, &val_buf) catch |err| {
            const result: CommandResult = switch (err) {
                error.NotLeader => .{ .err = .{ .code = .unavailable, .message = "not leader" } },
                else => .{ .err = .{ .code = .internal_error, .message = "propose failed" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, result);
            return;
        };

        applyCommittedEntries(shard);
        shard.waiter_pool.notify(.kv_get, qkey, @import("../node/shard.zig").resolveKVWaiter, @ptrCast(shard));

        const entry = shard.kv_handler.*.kv.get(qkey) orelse {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .internal_error, .message = "incr: post-apply lookup failed" } });
            return;
        };
        // Surface overflow to the client: applyIncr swallowed it during apply.
        if (entry.value.len != 8) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .internal_error, .message = "incr: counter corrupted" } });
            return;
        }
        sendKVResponse(shard, conn, req.header.request_id, .{ .kv_value = .{ .value = entry.value, .version = entry.version } });
    }

    /// TOUCH — update TTL of an existing key.
    /// Wire format: value = 8-byte u64 LE ttl_seconds (0 = clear / PERSIST).
    fn dispatchTouch(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        touchOrPersist(shard_ptr, conn_ptr, req, false);
    }

    /// PERSIST — clear TTL on an existing key. Ignores any value payload.
    fn dispatchPersist(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        touchOrPersist(shard_ptr, conn_ptr, req, true);
    }

    fn touchOrPersist(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request, force_persist: bool) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        if (validateKeySize(req.namespace, req.key)) |err_msg| {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = err_msg } });
            return;
        }
        if (req.key.len == 0) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "key is required" } });
            return;
        }
        if (isReservedKey(req.key)) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } });
            return;
        }

        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } });
            return;
        };

        // Pre-check existence so we can return not-found without a useless
        // Raft round-trip.
        const existing_for_touch = shard.kv_handler.*.kv.get(qkey);
        if (existing_for_touch == null) {
            sendKVResponse(shard, conn, req.header.request_id, .kv_not_found);
            return;
        }

        // CAS check — only the holder of the expected version may touch/persist.
        if (req.getCasVersion()) |expected_version| {
            if (existing_for_touch.?.version != expected_version) {
                sendKVResponse(shard, conn, req.header.request_id, .{ .kv_cas_failed = .{ .current_version = existing_for_touch.?.version } });
                return;
            }
        }

        // Compute absolute expiry_ns from ttl_seconds, or 0 to clear.
        var expiry_ns: u64 = 0;
        if (!force_persist) {
            var ttl_seconds: u64 = 0;
            if (req.value.len == 8) {
                ttl_seconds = std.mem.readInt(u64, req.value[0..8], .little);
            } else if (req.value.len != 0) {
                sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "touch: value must be empty or 8-byte u64 LE" } });
                return;
            }
            if (ttl_seconds > 0) {
                const now_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
                expiry_ns = now_ns + ttl_seconds * 1_000_000_000;
            }
        }

        var val_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &val_buf, expiry_ns, .little);

        // Inside a transaction? Buffer touch/persist.
        const txn_op_kind: txn_mod.TxnOpKind = if (force_persist) .persist else .touch;
        if (tryHandleInTxn(shard, conn, req, qkey, txn_op_kind, &val_buf, expiry_ns)) return;

        _ = proposeKVEntryWithValue(shard, .kv_touch, req, qkey, &val_buf) catch |err| {
            const result: CommandResult = switch (err) {
                error.NotLeader => .{ .err = .{ .code = .unavailable, .message = "not leader" } },
                else => .{ .err = .{ .code = .internal_error, .message = "propose failed" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, result);
            return;
        };

        applyCommittedEntries(shard);
        sendKVResponse(shard, conn, req.header.request_id, .ok);
    }

    /// EXISTS — return a 1-byte payload (0x00 or 0x01) wrapped in a kv_value
    /// envelope. version field is 0 when missing, otherwise the entry version.
    fn dispatchExists(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        if (validateKeySize(req.namespace, req.key)) |err_msg| {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = err_msg } });
            return;
        }

        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } });
            return;
        };

        const S = struct {
            threadlocal var byte: [1]u8 = undefined;
        };
        // Inside a transaction? Check buffered ops first.
        switch (tryReadInTxn(shard, conn, req, qkey)) {
            .none => {},
            .err => return,
            .deleted => {
                S.byte[0] = 0;
                sendKVResponse(shard, conn, req.header.request_id, .{ .kv_value = .{ .value = S.byte[0..1], .version = 0 } });
                return;
            },
            .value => {
                S.byte[0] = 1;
                sendKVResponse(shard, conn, req.header.request_id, .{ .kv_value = .{ .value = S.byte[0..1], .version = 0 } });
                return;
            },
            .miss => {},
        }
        if (shard.kv_handler.*.kv.get(qkey)) |entry| {
            S.byte[0] = 1;
            sendKVResponse(shard, conn, req.header.request_id, .{ .kv_value = .{ .value = S.byte[0..1], .version = entry.version } });
        } else {
            S.byte[0] = 0;
            sendKVResponse(shard, conn, req.header.request_id, .{ .kv_value = .{ .value = S.byte[0..1], .version = 0 } });
        }
    }

    /// JSON.GET — extract a path from a JSON-encoded value.
    /// Wire format: key = the KV key, value = the path expression (e.g. "$.name").
    fn dispatchJsonGet(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        if (validateKeySize(req.namespace, req.key)) |err_msg| {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = err_msg } });
            return;
        }

        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } });
            return;
        };

        const entry = shard.kv_handler.*.kv.get(qkey) orelse {
            sendKVResponse(shard, conn, req.header.request_id, .kv_not_found);
            return;
        };

        const path: []const u8 = if (req.value.len == 0) "$" else req.value;

        const json_path = @import("../util/json_path.zig");
        const result_bytes = json_path.jsonPathGet(shard.kv_handler.*.allocator, entry.value, path) catch |err| {
            const cmd: CommandResult = switch (err) {
                error.PathNotFound, error.NotAnObject, error.NotAnArray, error.IndexOutOfBounds => .kv_not_found,
                error.InvalidPath, error.InvalidJson => .{ .err = .{ .code = .invalid_request, .message = "invalid json path or document" } },
                error.OutOfMemory => .{ .err = .{ .code = .internal_error, .message = "out of memory" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, cmd);
            return;
        };
        defer shard.kv_handler.*.allocator.free(result_bytes);

        // Wire format: [version:u64 LE][result_bytes] — clients receive the
        // document version alongside the extracted JSON sub-value.
        const out = shard.kv_handler.*.allocator.alloc(u8, 8 + result_bytes.len) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .internal_error, .message = "out of memory" } });
            return;
        };
        defer shard.kv_handler.*.allocator.free(out);
        std.mem.writeInt(u64, out[0..8], entry.version, .little);
        if (result_bytes.len > 0) @memcpy(out[8..], result_bytes);
        sendKVResponse(shard, conn, req.header.request_id, .{ .kv_scan_result = .{ .data = out } });
    }

    /// JSON.SET — set the JSON value at `path` (read-modify-write through Raft).
    /// Wire format: key = KV key, value = `[path_len:u16][path][json]`.
    fn dispatchJsonSet(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        if (validateKeySize(req.namespace, req.key)) |err_msg| {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = err_msg } });
            return;
        }
        if (isReservedKey(req.key)) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } });
            return;
        }

        // Parse [path_len:u16][path][json] from value.
        if (req.value.len < 2) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "json_set: missing path_len" } });
            return;
        }
        const path_len = std.mem.readInt(u16, req.value[0..2], .little);
        if (req.value.len < 2 + path_len) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "json_set: truncated path" } });
            return;
        }
        const path = req.value[2 .. 2 + path_len];
        const new_json = req.value[2 + path_len ..];

        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } });
            return;
        };

        const json_path = @import("../util/json_path.zig");
        const allocator = shard.kv_handler.*.allocator;

        // Compute new document. Root path on a missing key creates a new doc.
        const merged: []u8 = blk: {
            if (shard.kv_handler.*.kv.get(qkey)) |entry| {
                break :blk json_path.jsonPathSet(allocator, entry.value, path, new_json) catch |err| {
                    const cmd: CommandResult = switch (err) {
                        error.InvalidPath, error.InvalidJson => .{ .err = .{ .code = .invalid_request, .message = "invalid json path or document" } },
                        error.PathNotFound, error.NotAnObject, error.NotAnArray, error.IndexOutOfBounds => .kv_not_found,
                        error.OutOfMemory => .{ .err = .{ .code = .internal_error, .message = "out of memory" } },
                    };
                    sendKVResponse(shard, conn, req.header.request_id, cmd);
                    return;
                };
            }
            // No existing key. Only "$" makes sense — replace whole doc.
            if (path.len != 1 or path[0] != '$') {
                sendKVResponse(shard, conn, req.header.request_id, .kv_not_found);
                return;
            }
            // Validate it's well-formed JSON before persisting.
            const parsed = std.json.parseFromSlice(std.json.Value, allocator, new_json, .{}) catch {
                sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "invalid json document" } });
                return;
            };
            parsed.deinit();
            break :blk allocator.dupe(u8, new_json) catch {
                sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .internal_error, .message = "out of memory" } });
                return;
            };
        };
        defer allocator.free(merged);

        _ = proposeKVEntryWithValue(shard, .kv_put, req, qkey, merged) catch |err| {
            const result: CommandResult = switch (err) {
                error.NotLeader => .{ .err = .{ .code = .unavailable, .message = "not leader" } },
                else => .{ .err = .{ .code = .internal_error, .message = "propose failed" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, result);
            return;
        };

        applyCommittedEntries(shard);
        shard.waiter_pool.notify(.kv_get, qkey, @import("../node/shard.zig").resolveKVWaiter, @ptrCast(shard));

        const version = if (shard.kv_handler.*.kv.get(qkey)) |entry| entry.version else 1;
        sendKVResponse(shard, conn, req.header.request_id, .{ .kv_put_ok = .{ .version = version } });
    }

    /// JSON.DEL — remove a path. Path "$" deletes the whole key.
    /// Wire format: key = KV key, value = path expression.
    fn dispatchJsonDel(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        if (validateKeySize(req.namespace, req.key)) |err_msg| {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = err_msg } });
            return;
        }
        if (isReservedKey(req.key)) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } });
            return;
        }

        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } });
            return;
        };

        const path: []const u8 = if (req.value.len == 0) "$" else req.value;

        const entry = shard.kv_handler.*.kv.get(qkey) orelse {
            sendKVResponse(shard, conn, req.header.request_id, .kv_not_found);
            return;
        };

        // Path "$" → delete the entire key (use kv_delete entry).
        if (path.len == 1 and path[0] == '$') {
            _ = proposeKVEntry(shard, .kv_delete, req, qkey) catch |err| {
                const result: CommandResult = switch (err) {
                    error.NotLeader => .{ .err = .{ .code = .unavailable, .message = "not leader" } },
                    else => .{ .err = .{ .code = .internal_error, .message = "propose failed" } },
                };
                sendKVResponse(shard, conn, req.header.request_id, result);
                return;
            };
            applyCommittedEntries(shard);
            shard.waiter_pool.notify(.kv_get, qkey, @import("../node/shard.zig").resolveKVWaiter, @ptrCast(shard));
            sendKVResponse(shard, conn, req.header.request_id, .ok);
            return;
        }

        // Sub-path → read-modify-write as kv_put.
        const json_path = @import("../util/json_path.zig");
        const allocator = shard.kv_handler.*.allocator;
        const merged = json_path.jsonPathDel(allocator, entry.value, path) catch |err| {
            const cmd: CommandResult = switch (err) {
                error.InvalidPath, error.InvalidJson => .{ .err = .{ .code = .invalid_request, .message = "invalid json path or document" } },
                error.PathNotFound, error.NotAnObject, error.NotAnArray, error.IndexOutOfBounds => .kv_not_found,
                error.OutOfMemory => .{ .err = .{ .code = .internal_error, .message = "out of memory" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, cmd);
            return;
        };
        defer allocator.free(merged);

        _ = proposeKVEntryWithValue(shard, .kv_put, req, qkey, merged) catch |err| {
            const result: CommandResult = switch (err) {
                error.NotLeader => .{ .err = .{ .code = .unavailable, .message = "not leader" } },
                else => .{ .err = .{ .code = .internal_error, .message = "propose failed" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, result);
            return;
        };

        applyCommittedEntries(shard);
        shard.waiter_pool.notify(.kv_get, qkey, @import("../node/shard.zig").resolveKVWaiter, @ptrCast(shard));
        sendKVResponse(shard, conn, req.header.request_id, .ok);
    }

    // ── Shard Walker: Local Scan ──────────────────────────────────────

    /// ShardWalker LocalScanFn for kv_scan — scans key names from one shard's
    /// KVProjection. Returns borrowed references (zero allocation).
    /// Filters reserved keys and strips namespace prefix.
    /// When filter is non-empty, only returns keys matching the prefix filter.
    fn localScanKeys(
        ctx: *anyopaque,
        namespace: []const u8,
        filter: []const u8,
        _: ?[]const u8,
        limit: u32,
    ) dispatcher_mod.NameWalker.ScanResult {
        const kv: *KVProjection = @ptrCast(@alignCast(ctx));
        const S = struct {
            threadlocal var key_buf: [1024][]const u8 = undefined;
            threadlocal var ns_buf: [MAX_QUALIFIED_KEY]u8 = undefined;
        };

        // Build scan prefix: namespace prefix + optional filter
        const ns_prefix = nsPrefix(&S.ns_buf, namespace);
        var scan_prefix = ns_prefix;
        if (filter.len > 0 and ns_prefix.len + filter.len <= S.ns_buf.len) {
            @memcpy(S.ns_buf[ns_prefix.len..][0..filter.len], filter);
            scan_prefix = S.ns_buf[0 .. ns_prefix.len + filter.len];
        }

        // Scan qualified key names from projection (filtered by prefix)
        const raw_count = kv.scanKeyNames(scan_prefix, &S.key_buf);

        // Strip namespace prefix and filter reserved keys in-place
        var count: usize = 0;
        const cap: usize = if (limit > 0) @intCast(limit) else S.key_buf.len;
        for (S.key_buf[0..raw_count]) |key| {
            if (count >= cap) break;
            const stripped = stripNsPrefix(key, namespace);
            if (!isReservedKey(stripped)) {
                S.key_buf[count] = stripped;
                count += 1;
            }
        }

        return .{ .items = S.key_buf[0..count], .next_cursor = null };
    }

    fn dispatchScan(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.kv_handler.*.handleCommand(req);
        defer shard.kv_handler.*.freeResult(cmd_result);
        sendKVResponse(shard, conn, req.header.request_id, cmd_result);
    }

    fn dispatchHistory(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.kv_handler.*.handleCommand(req);
        defer shard.kv_handler.*.freeResult(cmd_result);
        sendKVResponse(shard, conn, req.header.request_id, cmd_result);
    }

    // ── Multi-GET (mget) ─────────────────────────────────────────────────

    /// Maximum keys in a single MGET request.
    const MAX_MGET_KEYS = 256;

    /// Maximum response buffer for MGET (256 keys × average ~4KB = ~1MB).
    const MAX_MGET_RESPONSE = 1024 * 1024;

    /// Handle a batch GET request: look up multiple keys in a single round trip.
    /// Keys may span multiple shards — uses peer_shards for cross-shard reads.
    ///
    /// Request wire format (in value field):
    ///   [count:u16] ([key_len:u16][key])*
    ///
    /// Response wire format:
    ///   [count:u32] ([status:u8][key_len:u16][key][version:u64][value_len:u32][value])*
    ///   status: 0 = found, 2 = not_found
    fn dispatchMget(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        // Parse key count from value field
        if (req.value.len < 2) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "mget: missing key count" } });
            return;
        }

        const count = std.mem.readInt(u16, req.value[0..2], .little);
        if (count == 0) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "mget: zero keys" } });
            return;
        }
        if (count > MAX_MGET_KEYS) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "mget: too many keys (max 256)" } });
            return;
        }

        // Parse all keys from the packed value field
        var keys: [MAX_MGET_KEYS][]const u8 = undefined;
        var offset: usize = 2;
        for (0..count) |i| {
            if (offset + 2 > req.value.len) {
                sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "mget: truncated key list" } });
                return;
            }
            const key_len = std.mem.readInt(u16, req.value[offset..][0..2], .little);
            offset += 2;
            if (offset + key_len > req.value.len) {
                sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "mget: truncated key data" } });
                return;
            }
            keys[i] = req.value[offset..][0..key_len];
            offset += key_len;
        }

        // Build response: [count:u32] ([status:u8][key_len:u16][key][version:u64][value_len:u32][value])*
        var resp_buf: [MAX_MGET_RESPONSE]u8 = undefined;
        var resp_offset: usize = 4; // reserve space for count header

        for (0..count) |i| {
            const raw_key = keys[i];

            // Namespace-qualify key
            var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
            const qkey = qualifyKey(&qbuf, req.namespace, raw_key) catch {
                // Key too large — treat as not found
                if (resp_offset + 1 + 2 + raw_key.len + 8 + 4 > resp_buf.len) break;
                resp_buf[resp_offset] = 2; // not_found
                resp_offset += 1;
                std.mem.writeInt(u16, resp_buf[resp_offset..][0..2], @intCast(raw_key.len), .little);
                resp_offset += 2;
                @memcpy(resp_buf[resp_offset..][0..raw_key.len], raw_key);
                resp_offset += raw_key.len;
                std.mem.writeInt(u64, resp_buf[resp_offset..][0..8], 0, .little);
                resp_offset += 8;
                std.mem.writeInt(u32, resp_buf[resp_offset..][0..4], 0, .little);
                resp_offset += 4;
                continue;
            };

            // Route key to the correct shard
            const lookup_result = lookupKeyOnShard(shard, req.namespace, raw_key, qkey);

            // Serialize result entry
            const value_data = if (lookup_result.found) lookup_result.value else &[_]u8{};
            const entry_size = 1 + 2 + raw_key.len + 8 + 4 + value_data.len;
            if (resp_offset + entry_size > resp_buf.len) break; // response buffer full

            resp_buf[resp_offset] = if (lookup_result.found) 0 else 2;
            resp_offset += 1;
            std.mem.writeInt(u16, resp_buf[resp_offset..][0..2], @intCast(raw_key.len), .little);
            resp_offset += 2;
            @memcpy(resp_buf[resp_offset..][0..raw_key.len], raw_key);
            resp_offset += raw_key.len;
            std.mem.writeInt(u64, resp_buf[resp_offset..][0..8], lookup_result.version, .little);
            resp_offset += 8;
            std.mem.writeInt(u32, resp_buf[resp_offset..][0..4], @intCast(value_data.len), .little);
            resp_offset += 4;
            if (value_data.len > 0) {
                @memcpy(resp_buf[resp_offset..][0..value_data.len], value_data);
                resp_offset += value_data.len;
            }
        }

        // Write count header
        std.mem.writeInt(u32, resp_buf[0..4], @intCast(count), .little);

        log.debug("KV MGET: count={d}, response_size={d}", .{ count, resp_offset });
        sendKVResponse(shard, conn, req.header.request_id, .{ .kv_mget_result = .{ .data = resp_buf[0..resp_offset] } });
    }

    const MgetLookup = struct {
        found: bool,
        value: []const u8,
        version: u64,
    };

    /// Look up a single key, routing to the correct shard via peer_shards.
    fn lookupKeyOnShard(shard: *Shard, namespace: []const u8, raw_key: []const u8, qkey: []const u8) MgetLookup {
        const hash = router.hashKeyWithNamespace(namespace, raw_key);
        const target_shard_id = shard.router.partitionToShard(shard.router.hashToPartition(hash));

        if (target_shard_id == shard.id) {
            // Local lookup
            if (shard.kv_handler.*.kv.get(qkey)) |entry| {
                return .{ .found = true, .value = entry.value, .version = entry.version };
            }
            return .{ .found = false, .value = &[_]u8{}, .version = 0 };
        }

        // Cross-shard lookup via peer_shards
        if (shard.peer_shards) |peers| {
            if (target_shard_id < peers.len) {
                if (peers[target_shard_id].kv_handler.*.kv.get(qkey)) |entry| {
                    return .{ .found = true, .value = entry.value, .version = entry.version };
                }
            }
        }
        return .{ .found = false, .value = &[_]u8{}, .version = 0 };
    }

    // ── Raft Propose ────────────────────────────────────────────────────

    /// Build a CommandPayload from the request, set entry flags (TTL, tombstone),
    /// and propose the entry through RaftNode. Returns the ProposeResult with
    /// .index (becomes the entry version) and .term.
    ///
    /// After this returns successfully, call `applyCommittedEntries()` to apply
    /// any newly committed entries to the KV projection.
    fn proposeKVEntry(shard: *Shard, entry_type: entry_mod.EntryType, req: Request, qualified_key: []const u8) !@import("../raft/node.zig").ProposeResult {
        const value: []const u8 = if (entry_type == .kv_delete) &[_]u8{} else req.value;
        return proposeKVEntryWithValue(shard, entry_type, req, qualified_key, value);
    }

    /// Like `proposeKVEntry` but uses a caller-supplied value override instead
    /// of `req.value`. Used by INCR (8-byte i64 LE delta) and TOUCH/PERSIST
    /// (8-byte u64 LE absolute expiry_ns).
    fn proposeKVEntryWithValue(
        shard: *Shard,
        entry_type: entry_mod.EntryType,
        req: Request,
        qualified_key: []const u8,
        value: []const u8,
    ) !@import("../raft/node.zig").ProposeResult {
        // Build CommandPayload (namespace_hash:4 + key_len:2 + val_len:4 + key + value)
        var payload_buf: [MAX_ENTRY_PAYLOAD]u8 = undefined;
        const cmd = entry_mod.CommandPayload{
            .namespace_hash = router.namespaceHash(req.namespace),
            .key_length = @intCast(qualified_key.len),
            .value_length = @intCast(value.len),
            .key = qualified_key,
            .value = value,
        };
        var payload_len = cmd.serialize(&payload_buf) orelse return error.PayloadTooLarge;

        // Set flags and append TTL if present
        var flags: u16 = entry_mod.Flags.NONE;
        if (entry_type == .kv_delete) {
            flags |= entry_mod.Flags.TOMBSTONE;
        }

        const timestamp_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;

        if (entry_type == .kv_put) {
            if (req.getTtlSeconds()) |ttl_secs| {
                if (ttl_secs > 0) {
                    flags |= entry_mod.Flags.HAS_TTL;
                    const expiry_ns = timestamp_ns + ttl_secs * 1_000_000_000;
                    std.mem.writeInt(u64, payload_buf[payload_len..][0..8], expiry_ns, .little);
                    payload_len += 8;
                }
            }
        }

        // Propose through Raft — in single-node mode this commits immediately
        const propose_result = try shard.raft_node.propose(entry_type, flags, timestamp_ns, payload_buf[0..payload_len]);

        // Broadcast to cluster peers via raft network
        if (shard.raft_network) |rn| {
            if (shard.raft_node.log.getEntry(propose_result.index)) |committed_entry| {
                var entry_buf: [MAX_ENTRY_PAYLOAD + 64]u8 = undefined;
                if (committed_entry.serialize(&entry_buf)) |serialized_len| {
                    rn.broadcastEntry(entry_buf[0..serialized_len]) catch {};
                }
            }
        }

        return propose_result;
    }

    /// Apply all entries committed by Raft (commit_index > last_applied) to the
    /// KV projection. In single-node mode this is a tight synchronous loop since
    /// propose() advances commit_index immediately.
    fn applyCommittedEntries(shard: *Shard) void {
        const raft = shard.raft_node;
        while (raft.last_applied < raft.commit_index) {
            const next_idx = raft.last_applied + 1;
            // Grab Entry from RaftLog (may have payload on stack — getEntry borrows from UAL ring)
            if (raft.log.getEntry(next_idx)) |e| {
                shard.defaultPartition().kv.applyEntry(&e) catch {};
                raft.last_applied = next_idx;
            } else {
                // Entry evicted from ring — still advance last_applied to avoid stall
                raft.last_applied = next_idx;
            }
        }
    }

    // ── Per-Shard Transactions ─────────────────────────────────────────
    //
    // A transaction is opened with BEGIN(routing_key). The dispatcher's
    // `preRouteByKey` hook routes BEGIN to the partition that owns
    // `hash(namespace, routing_key)`. The TxnState stores that hash as
    // `pinned_hash`, and every subsequent op carrying `txn_id` must hash
    // to the same value or fail with `kv_txn_cross_shard`.
    //
    // Buffered ops are kept in `TxnTable.appendOp` (key/value owned by the
    // table). On COMMIT we serialize the entire write set as one `kv_batch`
    // UAL entry and propose it through Raft \u2014 atomicity is the same as any
    // single Raft propose. ROLLBACK simply drops the in-memory state.

    fn dispatchBeginTxn(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        const routing_key: []const u8 = if (req.findOption(.routing_key)) |opt|
            opt.asString()
        else
            req.key;

        if (routing_key.len == 0) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{
                .code = .invalid_request,
                .message = "BEGIN: routing key required (use --routing-key or supply key)",
            } });
            return;
        }

        const pinned_hash = router.hashKeyWithNamespace(req.namespace, routing_key);
        const ns_hash = router.namespaceHash(req.namespace);

        // owner_conn_id = 0 means "unowned" — txn outlives the originating
        // connection. Stateless CLI clients open a fresh TCP connection per
        // command, so binding to conn.id would auto-rollback at command exit.
        // Connection-bound txns can be added later via an explicit option.
        const txn_id = shard.kv_handler.*.txn_table.begin(pinned_hash, ns_hash, 0) catch |err| {
            const result: CommandResult = switch (err) {
                error.TooManyOpenTxns => .{ .err = .{ .code = .kv_txn_too_large, .message = "too many open transactions" } },
                else => .{ .err = .{ .code = .internal_error, .message = "begin failed" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, result);
            return;
        };

        log.debug("KV BEGIN: txn_id={d} pinned_hash={x} ns_hash={x}", .{ txn_id, pinned_hash, ns_hash });
        sendKVResponse(shard, conn, req.header.request_id, .{ .kv_txn_begin_ok = .{
            .txn_id = txn_id,
            .pinned_hash = pinned_hash,
        } });
    }

    fn dispatchCommitTxn(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        const txn_id = extractTxnId(req) orelse {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{
                .code = .invalid_request,
                .message = "COMMIT: txn_id required",
            } });
            return;
        };

        const txn_table = &shard.kv_handler.*.txn_table;
        const txn_state = txn_table.get(txn_id) orelse {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{
                .code = .kv_txn_unknown,
                .message = "transaction not found",
            } });
            return;
        };

        const op_count: u16 = @intCast(txn_state.ops.items.len);

        // Empty commit is a no-op success \u2014 just drop the txn.
        if (op_count == 0) {
            txn_table.drop(txn_id);
            sendKVResponse(shard, conn, req.header.request_id, .{ .kv_txn_commit_ok = .{
                .commit_index = shard.raft_node.commit_index,
                .op_count = 0,
            } });
            return;
        }

        // Serialize the batched op list. Heap-allocate \u2014 it can exceed stack
        // and we already enforce MAX_PAYLOAD_PER_TXN on append.
        const need = txn_mod.batchPayloadSize(txn_state.ops.items);
        const payload_buf = shard.kv_handler.*.allocator.alloc(u8, need) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .internal_error, .message = "commit: oom" } });
            return;
        };
        defer shard.kv_handler.*.allocator.free(payload_buf);

        const written = txn_mod.serializeBatch(payload_buf, txn_state.namespace_hash, txn_state.ops.items) catch {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .internal_error, .message = "commit: serialize failed" } });
            return;
        };

        const timestamp_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        const propose_result = shard.raft_node.propose(.kv_batch, entry_mod.Flags.NONE, timestamp_ns, payload_buf[0..written]) catch |err| {
            const result: CommandResult = switch (err) {
                error.NotLeader => .{ .err = .{ .code = .unavailable, .message = "not leader" } },
                else => .{ .err = .{ .code = .internal_error, .message = "commit: propose failed" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, result);
            return;
        };

        // Broadcast to peers (mirrors proposeKVEntryWithValue).
        if (shard.raft_network) |rn| {
            if (shard.raft_node.log.getEntry(propose_result.index)) |committed_entry| {
                var entry_buf: [MAX_ENTRY_PAYLOAD + 64]u8 = undefined;
                if (committed_entry.serialize(&entry_buf)) |serialized_len| {
                    rn.broadcastEntry(entry_buf[0..serialized_len]) catch {};
                }
            }
        }

        applyCommittedEntries(shard);

        // Drop the txn state \u2014 it's now durably committed.
        txn_table.drop(txn_id);

        log.debug("KV COMMIT: txn_id={d} ops={d} commit_index={d}", .{ txn_id, op_count, propose_result.index });
        sendKVResponse(shard, conn, req.header.request_id, .{ .kv_txn_commit_ok = .{
            .commit_index = propose_result.index,
            .op_count = op_count,
        } });
    }

    fn dispatchRollbackTxn(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        const txn_id = extractTxnId(req) orelse {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{
                .code = .invalid_request,
                .message = "ROLLBACK: txn_id required",
            } });
            return;
        };

        const txn_table = &shard.kv_handler.*.txn_table;
        if (txn_table.get(txn_id) == null) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{
                .code = .kv_txn_unknown,
                .message = "transaction not found",
            } });
            return;
        }
        txn_table.drop(txn_id);
        log.debug("KV ROLLBACK: txn_id={d}", .{txn_id});
        sendKVResponse(shard, conn, req.header.request_id, .ok);
    }

    /// Extract the transaction id from a request. Looks first at the
    /// `txn_id` TLV option, then falls back to the value field as an
    /// 8-byte little-endian u64 (CLI convenience).
    fn extractTxnId(req: Request) ?u64 {
        if (req.findOption(.txn_id)) |opt| {
            if (opt.asU64()) |v| return v;
        }
        if (req.value.len == 8) return std.mem.readInt(u64, req.value[0..8], .little);
        return null;
    }

    /// If the request carries a `txn_id` option, append it to the matching
    /// txn (for write ops) or surface read-your-writes (for read ops) and
    /// return true \u2014 the caller should not run the normal Raft path.
    /// Returns false when there is no txn_id, in which case the caller
    /// proceeds with normal dispatch.
    ///
    /// On any txn-related error (unknown txn, cross-shard, too large, etc.)
    /// this also returns true \u2014 the error response has already been sent.
    fn tryHandleInTxn(
        shard: *Shard,
        conn: *Connection,
        req: Request,
        qkey: []const u8,
        op_kind: txn_mod.TxnOpKind,
        op_value: []const u8,
        expiry_ns: u64,
    ) bool {
        const txn_opt = req.findOption(.txn_id) orelse return false;
        const txn_id = txn_opt.asU64() orelse {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "txn_id option must be u64" } });
            return true;
        };

        const txn_table = &shard.kv_handler.*.txn_table;
        const txn_state = txn_table.get(txn_id) orelse {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_txn_unknown, .message = "transaction not found" } });
            return true;
        };

        // Cross-shard guard: req must hash to the txn's pinned partition.
        const expected_hash = preRouteByKey(req) orelse 0;
        if (txn_state.pinned_hash != expected_hash) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{
                .code = .kv_txn_cross_shard,
                .message = "key hashes to a different partition than the transaction",
            } });
            return true;
        }

        // Single-namespace guard.
        const req_ns_hash = router.namespaceHash(req.namespace);
        if (txn_state.namespace_hash != req_ns_hash) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{
                .code = .kv_txn_unsupported_op,
                .message = "transaction is bound to a different namespace",
            } });
            return true;
        }

        // Read-your-writes for GET/EXISTS: check the buffered op log first.
        // For reads, op_kind is `.put` placeholder \u2014 caller will indicate via op_value.len?
        // We use a dedicated branch in the per-handler call sites instead.

        txn_table.appendOp(txn_id, op_kind, qkey, op_value, expiry_ns) catch |err| {
            const result: CommandResult = switch (err) {
                error.TxnTooLarge => .{ .err = .{ .code = .kv_txn_too_large, .message = "transaction op or payload limit exceeded" } },
                error.OutOfMemory => .{ .err = .{ .code = .internal_error, .message = "oom buffering txn op" } },
            };
            sendKVResponse(shard, conn, req.header.request_id, result);
            return true;
        };

        // Synthesize a per-op response. Real version will be assigned at COMMIT.
        const synthetic: CommandResult = switch (op_kind) {
            .put => .{ .kv_put_ok = .{ .version = 0 } },
            .delete, .touch, .persist => .ok,
            .incr => .{ .kv_value = .{ .value = op_value, .version = 0 } },
        };
        sendKVResponse(shard, conn, req.header.request_id, synthetic);
        return true;
    }

    /// Read-your-writes path: look up `qkey` in the buffered op log of the
    /// current txn (if any). Returns:
    ///   .none      \u2014 no txn id on the request, caller proceeds with normal read
    ///   .deleted   \u2014 txn has a buffered delete for this key (return not_found)
    ///   .value     \u2014 txn has a buffered put; slice borrows from the txn buffer
    ///   .miss      \u2014 has txn but no buffered op for this key, fall through to projection
    ///   .err       \u2014 txn-id error (unknown/cross-shard); response already sent
    const TxnReadOutcome = union(enum) {
        none,
        deleted,
        value: []const u8,
        miss,
        err,
    };
    fn tryReadInTxn(shard: *Shard, conn: *Connection, req: Request, qkey: []const u8) TxnReadOutcome {
        const txn_opt = req.findOption(.txn_id) orelse return .none;
        const txn_id = txn_opt.asU64() orelse {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "txn_id option must be u64" } });
            return .err;
        };
        const txn_table = &shard.kv_handler.*.txn_table;
        const txn_state = txn_table.get(txn_id) orelse {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_txn_unknown, .message = "transaction not found" } });
            return .err;
        };
        const expected_hash = preRouteByKey(req) orelse 0;
        if (txn_state.pinned_hash != expected_hash) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_txn_cross_shard, .message = "key hashes to a different partition than the transaction" } });
            return .err;
        }
        const req_ns_hash = router.namespaceHash(req.namespace);
        if (txn_state.namespace_hash != req_ns_hash) {
            sendKVResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .kv_txn_unsupported_op, .message = "transaction is bound to a different namespace" } });
            return .err;
        }
        const last = txn_table.lastOpForKey(txn_id, qkey) orelse return .miss;
        return switch (last.kind) {
            .delete => .deleted,
            .put => .{ .value = last.value },
            // For touch/persist/incr, the value isn't authoritative \u2014 fall through.
            .touch, .persist, .incr => .miss,
        };
    }

    // ── Core Command Logic ─────────────────────────────────────────────

    /// Dispatch a KV command to the appropriate handler.
    /// Used for read operations and direct/test writes (bypasses Raft).
    /// Production write path goes through dispatchPut/dispatchDelete → proposeKVEntry.
    pub fn handleCommand(self: *KVHandler, req: Request) CommandResult {
        const op: OpCode = @enumFromInt(req.header.op_code);
        return switch (op) {
            .kv_get => self.handleGet(req),
            .kv_put => self.handlePutDirect(req),
            .kv_delete => self.handleDeleteDirect(req),
            .kv_scan => self.handleScan(req),
            .kv_history => self.handleHistory(req),
            .kv_incr => self.handleIncrDirect(req),
            .kv_touch => self.handleTouchDirect(req, false),
            .kv_persist => self.handleTouchDirect(req, true),
            .kv_exists => self.handleExistsDirect(req),
            .kv_json_get => self.handleJsonGetDirect(req),
            .kv_json_set => self.handleJsonSetDirect(req),
            .kv_json_del => self.handleJsonDelDirect(req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown KV opcode" } },
        };
    }

    // ── GET ─────────────────────────────────────────────────────────────

    fn handleGet(self: *KVHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }

        if (isReservedKey(req.key)) {
            return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };
        }

        // Namespace-qualify key for projection lookup
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch
            return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } };

        const entry = self.kv.get(qkey) orelse return .kv_not_found;
        return .{ .kv_value = .{ .value = entry.value, .version = entry.version } };
    }

    // ── PUT direct (used by handleCommand — test/internal path) ─────────

    /// Direct put to the KV projection. Bypasses Raft — used for testing and
    /// internal operations. Production writes go through proposeKVEntry().
    fn handlePutDirect(self: *KVHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }
        if (isReservedKey(req.key)) {
            return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };
        }

        // Namespace-qualify key for projection operations
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch
            return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } };

        // CAS check (qualified key)
        if (req.getCasVersion()) |expected_version| {
            const current = self.kv.get(qkey);
            if (current) |entry| {
                if (entry.version != expected_version) {
                    return .{ .kv_cas_failed = .{ .current_version = entry.version } };
                }
            } else {
                if (expected_version != 0) {
                    return .{ .kv_cas_failed = .{ .current_version = 0 } };
                }
            }
        }

        // NX / XX conditions (qualified key)
        if (req.getIfNotExists()) {
            if (self.kv.get(qkey) != null) return .kv_condition_not_met;
        }
        if (req.getIfExists()) {
            if (self.kv.get(qkey) == null) return .kv_condition_not_met;
        }

        const lsn = self.nextLsn();
        const timestamp = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        const expiry_ns: u64 = if (req.getTtlSeconds()) |ttl_secs| blk: {
            if (ttl_secs == 0) break :blk 0;
            break :blk timestamp + ttl_secs * 1_000_000_000;
        } else 0;

        self.kv.put(qkey, req.value, lsn, 0, timestamp, expiry_ns) catch {
            return .{ .err = .{ .code = .internal_error, .message = "put failed" } };
        };
        const version = if (self.kv.get(qkey)) |entry| entry.version else 1;
        return .{ .kv_put_ok = .{ .version = version } };
    }

    // ── DELETE direct (used by handleCommand — test/internal path) ───────

    /// Direct delete from the KV projection. Bypasses Raft.
    fn handleDeleteDirect(self: *KVHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }
        if (isReservedKey(req.key)) {
            return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };
        }

        // Namespace-qualify key for projection operations
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch
            return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } };

        // CAS check — only the holder of the expected version may delete.
        if (req.getCasVersion()) |expected_version| {
            if (self.kv.get(qkey)) |entry| {
                if (entry.version != expected_version) {
                    return .{ .kv_cas_failed = .{ .current_version = entry.version } };
                }
            } else {
                if (expected_version != 0) {
                    return .{ .kv_cas_failed = .{ .current_version = 0 } };
                }
            }
        }

        const lsn = self.nextLsn();
        const timestamp = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        self.kv.delete(qkey, lsn, 0, timestamp) catch {
            return .{ .err = .{ .code = .internal_error, .message = "delete failed" } };
        };
        return .ok;
    }

    // ── PUT validation (pre-checks only — no write to projection) ───────

    /// Validate a put request before proposing to Raft.
    /// Returns null if all checks pass, or a CommandResult error to send immediately.
    fn validatePut(self: *KVHandler, req: Request) ?CommandResult {
        return self.validatePutQ(req, req.key);
    }

    /// Validate a put request with a namespace-qualified key for projection lookups.
    fn validatePutQ(self: *KVHandler, req: Request, qkey: []const u8) ?CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }
        if (isReservedKey(req.key)) {
            return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };
        }

        // CAS check — version must match current (use qualified key for lookup)
        if (req.getCasVersion()) |expected_version| {
            const current = self.kv.get(qkey);
            if (current) |entry| {
                if (entry.version != expected_version) {
                    return .{ .kv_cas_failed = .{ .current_version = entry.version } };
                }
            } else {
                if (expected_version != 0) {
                    return .{ .kv_cas_failed = .{ .current_version = 0 } };
                }
            }
        }

        // NX: must not exist
        if (req.getIfNotExists()) {
            if (self.kv.get(qkey) != null) return .kv_condition_not_met;
        }

        // XX: must exist
        if (req.getIfExists()) {
            if (self.kv.get(qkey) == null) return .kv_condition_not_met;
        }

        return null; // all checks passed
    }

    // ── DELETE validation (pre-checks only — no write to projection) ─────

    /// Validate a delete request before proposing to Raft.
    fn validateDelete(_: *KVHandler, req: Request) ?CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }
        if (isReservedKey(req.key)) {
            return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };
        }
        return null; // all checks passed
    }

    // ── SCAN ────────────────────────────────────────────────────────────

    fn handleScan(self: *KVHandler, req: Request) CommandResult {
        const limit = req.getLimit() orelse DEFAULT_SCAN_LIMIT;
        const capped_limit = @min(limit, MAX_SCAN_LIMIT);

        // Namespace-qualify prefix for scanning using centralized namespace utilities.
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const ns = req.namespace;

        // Build qualified scan prefix:
        //   - If namespace + user prefix: "ns\0prefix"
        //   - If namespace + no prefix: "ns\0" (scan all in namespace)
        //   - If no namespace + user prefix: "prefix"
        //   - If no namespace + no prefix: full scan
        const scan_prefix: []const u8 = if (req.key.len > 0)
            qualifyKey(&qbuf, ns, req.key) catch
                return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } }
        else
            nsPrefix(&qbuf, ns);

        // Allocate scan buffer on stack
        var scan_buf: [MAX_SCAN_LIMIT]ScanEntry = undefined;
        const out = scan_buf[0..capped_limit];

        // Prefix scan if prefix is provided, otherwise full scan
        const found_count = if (scan_prefix.len > 0)
            self.kv.scanPrefix(scan_prefix, out)
        else
            self.kv.scan(out);

        // Filter out reserved keys and strip namespace prefix from results
        var filtered_count: usize = 0;
        for (out[0..found_count]) |entry| {
            if (!isReservedKey(entry.key)) {
                var stripped = entry;
                stripped.key = stripNsPrefix(entry.key, ns);
                scan_buf[filtered_count] = stripped;
                filtered_count += 1;
            }
        }

        const keys_only = req.getKeysOnly();

        // Serialize scan results
        const data = serializeScanResults(self.allocator, scan_buf[0..filtered_count], keys_only) catch {
            return .{ .err = .{ .code = .internal_error, .message = "scan serialization failed" } };
        };

        return .{ .kv_scan_result = .{ .data = data } };
    }

    // ── HISTORY ─────────────────────────────────────────────────────────

    fn handleHistory(self: *KVHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }

        if (isReservedKey(req.key)) {
            return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };
        }

        // Namespace-qualify key for projection lookup
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch
            return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } };

        // Fetch version history from projection
        var hist_buf: [kv_mod.DEFAULT_VERSION_CHAIN_LEN + 1]kv_mod.VersionEntry = undefined;
        const n = self.kv.getHistory(qkey, &hist_buf);

        if (n == 0) return .kv_not_found;

        // Serialize: [count:u32] ([value_len:u32][value][version:u64])*
        var total_size: usize = 4; // count header
        for (hist_buf[0..n]) |ver| {
            total_size += 4 + ver.value.len + 8; // value_len + value + version
        }

        const data = self.allocator.alloc(u8, total_size) catch
            return .{ .err = .{ .code = .internal_error, .message = "history serialization failed" } };

        var offset: usize = 0;
        std.mem.writeInt(u32, data[offset..][0..4], @intCast(n), .little);
        offset += 4;

        for (hist_buf[0..n]) |ver| {
            std.mem.writeInt(u32, data[offset..][0..4], @intCast(ver.value.len), .little);
            offset += 4;
            if (ver.value.len > 0) {
                @memcpy(data[offset..][0..ver.value.len], ver.value);
                offset += ver.value.len;
            }
            std.mem.writeInt(u64, data[offset..][0..8], ver.version, .little);
            offset += 8;
        }

        return .{ .kv_history_result = .{ .data = data } };
    }

    // ── Extended KV (Direct/RESP path — bypasses Raft) ─────────────────

    /// Direct INCR — operates on local KV. Used by RESP path. Default delta = 1
    /// when the value field is empty. Returns kv_value with 8-byte i64 LE.
    fn handleIncrDirect(self: *KVHandler, req: Request) CommandResult {
        if (req.key.len == 0) return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        if (isReservedKey(req.key)) return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };

        var delta: i64 = 1;
        if (req.value.len == 8) {
            delta = std.mem.readInt(i64, req.value[0..8], .little);
        } else if (req.value.len != 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "incr: value must be empty or 8-byte i64 LE" } };
        }

        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch
            return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } };

        if (self.kv.getRaw(qkey)) |existing| {
            if (!existing.tombstone and existing.value.len != 8) {
                return .{ .err = .{ .code = .invalid_request, .message = "value is not a counter" } };
            }
        }

        const lsn = self.nextLsn();
        const timestamp = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        _ = self.kv.applyIncr(qkey, delta, lsn, 0, timestamp) catch |err| {
            return switch (err) {
                error.Overflow => .{ .err = .{ .code = .invalid_request, .message = "counter overflow" } },
                error.NotACounter => .{ .err = .{ .code = .invalid_request, .message = "value is not a counter" } },
                error.OutOfMemory => .{ .err = .{ .code = .internal_error, .message = "out of memory" } },
            };
        };

        const entry = self.kv.get(qkey) orelse
            return .{ .err = .{ .code = .internal_error, .message = "incr: post-apply lookup failed" } };
        return .{ .kv_value = .{ .value = entry.value, .version = entry.version } };
    }

    /// Direct TOUCH/PERSIST — operates on local KV.
    fn handleTouchDirect(self: *KVHandler, req: Request, force_persist: bool) CommandResult {
        if (req.key.len == 0) return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        if (isReservedKey(req.key)) return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };

        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch
            return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } };

        const existing = self.kv.get(qkey) orelse return .kv_not_found;

        // CAS check — only the holder of the expected version may touch/persist.
        if (req.getCasVersion()) |expected_version| {
            if (existing.version != expected_version) {
                return .{ .kv_cas_failed = .{ .current_version = existing.version } };
            }
        }

        var expiry_ns: u64 = 0;
        if (!force_persist) {
            var ttl_seconds: u64 = 0;
            if (req.value.len == 8) {
                ttl_seconds = std.mem.readInt(u64, req.value[0..8], .little);
            } else if (req.value.len != 0) {
                return .{ .err = .{ .code = .invalid_request, .message = "touch: value must be empty or 8-byte u64 LE" } };
            }
            if (ttl_seconds > 0) {
                const now_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
                expiry_ns = now_ns + ttl_seconds * 1_000_000_000;
            }
        }

        const lsn = self.nextLsn();
        const timestamp = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        self.kv.applyTouch(qkey, expiry_ns, lsn, 0, timestamp) catch |err| switch (err) {
            error.NotFound => return .kv_not_found,
        };
        return .ok;
    }

    /// Direct EXISTS — returns kv_value with single byte 0/1.
    fn handleExistsDirect(self: *KVHandler, req: Request) CommandResult {
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch
            return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } };

        const S = struct {
            threadlocal var byte: [1]u8 = undefined;
        };
        if (self.kv.get(qkey)) |entry| {
            S.byte[0] = 1;
            return .{ .kv_value = .{ .value = S.byte[0..1], .version = entry.version } };
        }
        S.byte[0] = 0;
        return .{ .kv_value = .{ .value = S.byte[0..1], .version = 0 } };
    }

    /// Direct JSON.GET — owned bytes are returned via kv_value.value but the
    /// allocation lives until the next handler call. Caller (RESP path)
    /// serializes the response synchronously before this can be reused.
    fn handleJsonGetDirect(self: *KVHandler, req: Request) CommandResult {
        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch
            return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } };

        const entry = self.kv.get(qkey) orelse return .kv_not_found;

        const path: []const u8 = if (req.value.len == 0) "$" else req.value;
        const json_path = @import("../util/json_path.zig");
        const result_bytes = json_path.jsonPathGet(self.allocator, entry.value, path) catch |err| {
            return switch (err) {
                error.PathNotFound, error.NotAnObject, error.NotAnArray, error.IndexOutOfBounds => .kv_not_found,
                error.InvalidPath, error.InvalidJson => .{ .err = .{ .code = .invalid_request, .message = "invalid json path or document" } },
                error.OutOfMemory => .{ .err = .{ .code = .internal_error, .message = "out of memory" } },
            };
        };

        // Stash the allocation so freeResult() can free it.
        // Wire format: [version:u64 LE][result_bytes] — gives clients the
        // document version for CAS/causality, same shape as the RESP-style
        // version-prefixed envelope used by GET.
        const out = self.allocator.alloc(u8, 8 + result_bytes.len) catch {
            self.allocator.free(result_bytes);
            return .{ .err = .{ .code = .internal_error, .message = "out of memory" } };
        };
        std.mem.writeInt(u64, out[0..8], entry.version, .little);
        if (result_bytes.len > 0) @memcpy(out[8..], result_bytes);
        self.allocator.free(result_bytes);
        return .{ .kv_scan_result = .{ .data = out } };
    }

    /// Direct JSON.SET — value layout: [path_len:u16][path][json].
    fn handleJsonSetDirect(self: *KVHandler, req: Request) CommandResult {
        if (req.key.len == 0) return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        if (isReservedKey(req.key)) return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };

        if (req.value.len < 2) return .{ .err = .{ .code = .invalid_request, .message = "json_set: missing path_len" } };
        const path_len = std.mem.readInt(u16, req.value[0..2], .little);
        if (req.value.len < 2 + path_len) return .{ .err = .{ .code = .invalid_request, .message = "json_set: truncated path" } };
        const path = req.value[2 .. 2 + path_len];
        const new_json = req.value[2 + path_len ..];

        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch
            return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } };

        const json_path = @import("../util/json_path.zig");

        const merged: []u8 = blk: {
            if (self.kv.get(qkey)) |entry| {
                break :blk json_path.jsonPathSet(self.allocator, entry.value, path, new_json) catch |err| {
                    return switch (err) {
                        error.InvalidPath, error.InvalidJson => .{ .err = .{ .code = .invalid_request, .message = "invalid json path or document" } },
                        error.PathNotFound, error.NotAnObject, error.NotAnArray, error.IndexOutOfBounds => .kv_not_found,
                        error.OutOfMemory => .{ .err = .{ .code = .internal_error, .message = "out of memory" } },
                    };
                };
            }
            if (path.len != 1 or path[0] != '$') return .kv_not_found;
            const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, new_json, .{}) catch
                return .{ .err = .{ .code = .invalid_request, .message = "invalid json document" } };
            parsed.deinit();
            break :blk self.allocator.dupe(u8, new_json) catch
                return .{ .err = .{ .code = .internal_error, .message = "out of memory" } };
        };
        defer self.allocator.free(merged);

        const lsn = self.nextLsn();
        const timestamp = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        self.kv.put(qkey, merged, lsn, 0, timestamp, 0) catch
            return .{ .err = .{ .code = .internal_error, .message = "put failed" } };
        const version = if (self.kv.get(qkey)) |entry| entry.version else 1;
        return .{ .kv_put_ok = .{ .version = version } };
    }

    /// Direct JSON.DEL.
    fn handleJsonDelDirect(self: *KVHandler, req: Request) CommandResult {
        if (req.key.len == 0) return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        if (isReservedKey(req.key)) return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };

        var qbuf: [MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = qualifyKey(&qbuf, req.namespace, req.key) catch
            return .{ .err = .{ .code = .kv_key_too_large, .message = "namespace + key too large" } };

        const path: []const u8 = if (req.value.len == 0) "$" else req.value;
        const entry = self.kv.get(qkey) orelse return .kv_not_found;

        if (path.len == 1 and path[0] == '$') {
            const lsn = self.nextLsn();
            const timestamp = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
            self.kv.delete(qkey, lsn, 0, timestamp) catch
                return .{ .err = .{ .code = .internal_error, .message = "delete failed" } };
            return .ok;
        }

        const json_path = @import("../util/json_path.zig");
        const merged = json_path.jsonPathDel(self.allocator, entry.value, path) catch |err| {
            return switch (err) {
                error.InvalidPath, error.InvalidJson => .{ .err = .{ .code = .invalid_request, .message = "invalid json path or document" } },
                error.PathNotFound, error.NotAnObject, error.NotAnArray, error.IndexOutOfBounds => .kv_not_found,
                error.OutOfMemory => .{ .err = .{ .code = .internal_error, .message = "out of memory" } },
            };
        };
        defer self.allocator.free(merged);

        const lsn = self.nextLsn();
        const timestamp = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        self.kv.put(qkey, merged, lsn, 0, timestamp, 0) catch
            return .{ .err = .{ .code = .internal_error, .message = "put failed" } };
        const new_version = if (self.kv.get(qkey)) |new_entry| new_entry.version else 1;
        return .{ .kv_put_ok = .{ .version = new_version } };
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn nextLsn(self: *KVHandler) u64 {
        const lsn = self.next_lsn;
        self.next_lsn += 1;
        return lsn;
    }

    /// Free any heap-allocated data inside a CommandResult.
    /// Call this (via defer) after sending the response to the client.
    pub fn freeResult(self: *KVHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .kv_scan_result => |scan| {
                if (scan.data.len > 0) {
                    self.allocator.free(scan.data);
                }
            },
            .kv_history_result => |hist| {
                if (hist.data.len > 0) {
                    self.allocator.free(hist.data);
                }
            },
            else => {},
        }
    }

    // ── Blocking GET Waiter Management ──────────────────────────────────
    // (Moved to unified WaiterPool in node/waiter_pool.zig)
    // Handlers use shard.waiter_pool.register() / .notify() / .expireTimeouts()
};

// ═══════════════════════════════════════════════════════════════════════════════
// Response Serialization — CommandResult → Wire Response
// ═══════════════════════════════════════════════════════════════════════════════

/// Maximum response buffer: 24-byte header + 8-byte prefix + 256KB payload
const MAX_RESPONSE_BUF = @sizeOf(proto.ResponseHeader) + 8 + (256 * 1024);

/// Convert a CommandResult to a wire response and queue it on the connection.
/// Handles all KV result variants: kv_value (with version prefix), kv_put_ok,
/// kv_not_found, kv_cas_failed, kv_condition_not_met, kv_scan_result, ok, err.
fn sendKVResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .kv_value => |v| {
            // CLI expects: [version:u64 LE][value bytes]
            var resp = proto.Response.init(request_id, .ok, v.value);
            resp.prefix = v.version;
            var buf: [MAX_RESPONSE_BUF]u8 = undefined;
            const serialized = resp.serialize(&buf) catch return;
            _ = conn.queueWrite(serialized);
        },
        .kv_not_found => {
            var resp = proto.Response.initError(request_id, .not_found);
            var buf: [128]u8 = undefined;
            const serialized = resp.serialize(&buf) catch return;
            _ = conn.queueWrite(serialized);
        },
        .kv_put_ok => |v| {
            // Wire: [version:u64 LE]. Clients use this for CAS without a History round-trip.
            var resp = proto.Response.init(request_id, .ok, "");
            resp.prefix = v.version;
            var buf: [128]u8 = undefined;
            const serialized = resp.serialize(&buf) catch return;
            _ = conn.queueWrite(serialized);
        },
        .ok => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .kv_cas_failed => {
            var resp = proto.Response.initError(request_id, .conflict);
            var buf: [128]u8 = undefined;
            const serialized = resp.serialize(&buf) catch return;
            _ = conn.queueWrite(serialized);
        },
        .kv_condition_not_met => {
            var resp = proto.Response.initError(request_id, .conflict);
            var buf: [128]u8 = undefined;
            const serialized = resp.serialize(&buf) catch return;
            _ = conn.queueWrite(serialized);
        },
        .kv_scan_result => |scan| {
            shard.sendOkResponse(conn, request_id, scan.data);
        },
        .kv_history_result => |hist| {
            shard.sendOkResponse(conn, request_id, hist.data);
        },
        .kv_mget_result => |batch| {
            shard.sendOkResponse(conn, request_id, batch.data);
        },
        .kv_txn_begin_ok => |t| {
            // Wire payload: [variant:u8=0][txn_id:u64 LE][pinned_hash:u64 LE]
            var buf: [17]u8 = undefined;
            buf[0] = 0;
            std.mem.writeInt(u64, buf[1..9], t.txn_id, .little);
            std.mem.writeInt(u64, buf[9..17], t.pinned_hash, .little);
            shard.sendOkResponse(conn, request_id, &buf);
        },
        .kv_txn_commit_ok => |c| {
            // Wire payload: [variant:u8=1][commit_index:u64 LE][op_count:u16 LE]
            var buf: [11]u8 = undefined;
            buf[0] = 1;
            std.mem.writeInt(u64, buf[1..9], c.commit_index, .little);
            std.mem.writeInt(u16, buf[9..11], c.op_count, .little);
            shard.sendOkResponse(conn, request_id, &buf);
        },
        .err => |e| {
            const status = errorCodeToStatus(e.code);
            shard.sendErrorResponse(conn, request_id, status, e.message);
        },
        else => {
            shard.sendErrorResponse(conn, request_id, .internal_error, "unexpected result type");
        },
    }
}

/// Map CommandResult.ErrorCode to wire StatusCode.
fn errorCodeToStatus(code: CommandResult.ErrorCode) proto.StatusCode {
    return switch (code) {
        .invalid_request => .bad_request,
        .unauthorized => .unauthorized,
        .not_found => .not_found,
        .already_exists => .conflict,
        .timeout => .internal_error,
        .internal_error => .internal_error,
        .unavailable => .internal_error,
        .kv_key_too_large => .bad_request,
        .kv_value_too_large => .bad_request,
        .kv_namespace_not_found => .not_found,
        .kv_txn_unknown => .not_found,
        .kv_txn_cross_shard => .bad_request,
        .kv_txn_too_large => .bad_request,
        .kv_txn_timeout => .internal_error,
        .kv_txn_unsupported_op => .bad_request,
        .conflict => .conflict,
        else => .internal_error,
    };
}

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

/// Serialize scan results to binary format.
/// Wire format: [count:u32] ([key_len:u16][key][value_len:u32][value])* [has_more:u8]
/// When keys_only=true, value_len is 0 and value is empty (field is still present).
fn serializeScanResults(allocator: Allocator, entries: []const ScanEntry, keys_only: bool) ![]u8 {
    // Calculate total size
    var total: usize = 4; // count header
    for (entries) |entry| {
        total += 2 + entry.key.len; // key_len + key
        // Always include value_len field; when keys_only, value_len=0
        if (keys_only) {
            total += 4; // value_len only (0)
        } else {
            total += 4 + entry.value.len; // value_len + value
        }
    }
    total += 1; // has_more flag

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var offset: usize = 0;

    // Count
    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(entries.len), .little);
    offset += 4;

    for (entries) |entry| {
        // Key
        std.mem.writeInt(u16, buf[offset..][0..2], @intCast(entry.key.len), .little);
        offset += 2;
        @memcpy(buf[offset..][0..entry.key.len], entry.key);
        offset += entry.key.len;

        // Value — always present; empty when keys_only
        if (keys_only) {
            std.mem.writeInt(u32, buf[offset..][0..4], 0, .little);
            offset += 4;
        } else {
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(entry.value.len), .little);
            offset += 4;
            @memcpy(buf[offset..][0..entry.value.len], entry.value);
            offset += entry.value.len;
        }
    }

    // has_more — always false for now (no cursor pagination yet)
    buf[offset] = 0;

    return buf;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

/// Helper to build a test request for a given opcode.
fn makeRequest(op: OpCode, key: []const u8, value: []const u8, options: []const u8) Request {
    return .{
        .header = .{
            .magic = proto.MAGIC,
            .payload_length = 0,
            .request_id = 1,
            .crc32 = 0,
            .version = proto.VERSION,
            .op_code = @intFromEnum(op),
            .flags = 0,
            .reserved = .{0} ** 8,
        },
        .namespace = "default",
        .key = key,
        .value = value,
        .options = options,
    };
}

test "kv handler: get existing key" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    try kv.put("hello", "world", 1, 0, 1000, 0);

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_get, "hello", "", ""));

    switch (result) {
        .kv_value => |v| {
            try testing.expectEqualStrings("world", v.value);
            try testing.expectEqual(@as(u64, 1), v.version);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: get non-existent key" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_get, "nope", "", ""));

    switch (result) {
        .kv_not_found => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: get empty key returns error" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_get, "", "", ""));

    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: get reserved key blocked" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    // Manually put a reserved key (bypass handler)
    try kv.put("_sys:config", "secret", 1, 0, 1000, 0);

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_get, "_sys:config", "", ""));

    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.unauthorized, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put and get" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Put
    const put_result = handler.handleCommand(makeRequest(.kv_put, "mykey", "myval", ""));
    switch (put_result) {
        .kv_put_ok => |p| try testing.expectEqual(@as(u64, 1), p.version),
        else => return error.TestUnexpectedResult,
    }

    // Get it back
    const get_result = handler.handleCommand(makeRequest(.kv_get, "mykey", "", ""));
    switch (get_result) {
        .kv_value => |v| try testing.expectEqualStrings("myval", v.value),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put with CAS success" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Initial put (version = 1)
    _ = handler.handleCommand(makeRequest(.kv_put, "k", "v1", ""));

    // CAS put with version 1 → should succeed
    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addU64(.cas_version, 1);
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.kv_put, "k", "v2", opts));
    switch (result) {
        .kv_put_ok => {},
        else => return error.TestUnexpectedResult,
    }

    // Verify updated
    const get_res = handler.handleCommand(makeRequest(.kv_get, "k", "", ""));
    switch (get_res) {
        .kv_value => |v| try testing.expectEqualStrings("v2", v.value),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put with CAS failure" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Initial put (version = 1)
    _ = handler.handleCommand(makeRequest(.kv_put, "k", "v1", ""));

    // CAS put with wrong version 42 → should fail
    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addU64(.cas_version, 42);
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.kv_put, "k", "v2", opts));
    switch (result) {
        .kv_cas_failed => |c| try testing.expectEqual(@as(u64, 1), c.current_version),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put if_not_exists success" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addFlag(.if_not_exists);
    const opts = builder.getOptions();

    // Key doesn't exist → should succeed
    const result = handler.handleCommand(makeRequest(.kv_put, "new_key", "val", opts));
    switch (result) {
        .kv_put_ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put if_not_exists failure" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Pre-populate
    _ = handler.handleCommand(makeRequest(.kv_put, "existing", "val", ""));

    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addFlag(.if_not_exists);
    const opts = builder.getOptions();

    // Key exists → should fail
    const result = handler.handleCommand(makeRequest(.kv_put, "existing", "new_val", opts));
    switch (result) {
        .kv_condition_not_met => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put if_exists success" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Pre-populate
    _ = handler.handleCommand(makeRequest(.kv_put, "existing", "old", ""));

    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addFlag(.if_exists);
    const opts = builder.getOptions();

    // Key exists → should succeed
    const result = handler.handleCommand(makeRequest(.kv_put, "existing", "new", opts));
    switch (result) {
        .kv_put_ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put if_exists failure" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addFlag(.if_exists);
    const opts = builder.getOptions();

    // Key doesn't exist → should fail
    const result = handler.handleCommand(makeRequest(.kv_put, "missing", "val", opts));
    switch (result) {
        .kv_condition_not_met => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: delete" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Put then delete
    _ = handler.handleCommand(makeRequest(.kv_put, "k", "v", ""));
    const del_result = handler.handleCommand(makeRequest(.kv_delete, "k", "", ""));
    switch (del_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }

    // Get should return not_found
    const get_result = handler.handleCommand(makeRequest(.kv_get, "k", "", ""));
    switch (get_result) {
        .kv_not_found => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: delete reserved key blocked" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_delete, "_flo:metadata", "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.unauthorized, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: scan empty" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_scan, "", "", ""));

    switch (result) {
        .kv_scan_result => |r| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: scan with results" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Insert some keys
    _ = handler.handleCommand(makeRequest(.kv_put, "a", "1", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "b", "2", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "c", "3", ""));

    const result = handler.handleCommand(makeRequest(.kv_scan, "", "", ""));
    switch (result) {
        .kv_scan_result => |r| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 3), count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: scan with limit" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Insert 5 keys
    _ = handler.handleCommand(makeRequest(.kv_put, "a", "1", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "b", "2", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "c", "3", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "d", "4", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "e", "5", ""));

    // Scan with limit 2
    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addU32(.limit, 2);
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.kv_scan, "", "", opts));
    switch (result) {
        .kv_scan_result => |r| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expect(count <= 2);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: scan filters reserved keys" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    // Insert user key and reserved key directly into projection
    try kv.put("user_key", "val", 1, 0, 1000, 0);
    try kv.put("_sys:hidden", "secret", 2, 0, 2000, 0);

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_scan, "", "", ""));

    switch (result) {
        .kv_scan_result => |r| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, r.data[0..4], .little);
            // Only user_key should appear
            try testing.expectEqual(@as(u32, 1), count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    KVHandler.register(&dispatcher);

    // Verify handlers were registered for KV opcodes
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_get)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_put)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_delete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_scan)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_history)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_mget)] != null);

    // New extended KV opcodes (KV_ENHANCEMENTS phase 1).
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_incr)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_touch)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_persist)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_exists)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_json_get)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_json_set)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_json_del)] != null);

    // Verify pre-route hooks
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_get)] != null);
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_put)] != null);
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_scan)] == null); // walk-only, no pre-route
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_history)] == null); // no routing for history
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_mget)] == null); // multi-key, no pre-route

    // 6 original + 7 extended + 3 txn = 16 handlers registered
    try testing.expectEqual(@as(u16, 16), dispatcher.handler_count);
}

test "kv handler: pre-route by key" {
    const req1 = makeRequest(.kv_get, "key1", "", "");
    const req2 = makeRequest(.kv_get, "key1", "", "");
    const req3 = makeRequest(.kv_get, "key2", "", "");

    // Same key + same namespace → same hash
    const h1 = KVHandler.preRouteByKey(req1);
    const h2 = KVHandler.preRouteByKey(req2);
    try testing.expectEqual(h1, h2);

    // Different key + same namespace → different hash (with overwhelming probability)
    const h3 = KVHandler.preRouteByKey(req3);
    try testing.expect(h1 != h3);

    // Empty key → hash 0
    const req_empty = makeRequest(.kv_get, "", "", "");
    try testing.expectEqual(@as(?u64, 0), KVHandler.preRouteByKey(req_empty));

    // Same key, different namespace → different hash (namespace isolation)
    var req_ns = makeRequest(.kv_get, "key1", "", "");
    req_ns.namespace = "other";
    const h_other = KVHandler.preRouteByKey(req_ns);
    try testing.expect(h1 != h_other);
}

test "kv handler: history returns not found for missing key" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_history, "k", "", ""));
    try testing.expectEqual(CommandResult.kv_not_found, result);
}

test "kv handler: history returns versions" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    // Put two versions (unqualified key — default ns uses raw key)
    try kv.put("k", "v1", 1, 1, 100, 0);
    try kv.put("k", "v2", 2, 1, 200, 0);

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_history, "k", "", ""));
    defer handler.freeResult(result);
    switch (result) {
        .kv_history_result => |hist| {
            // Parse: [count:u32] ([value_len:u32][value][version:u64])*
            const data = hist.data;
            try testing.expect(data.len >= 4);
            const count = std.mem.readInt(u32, data[0..4], .little);
            try testing.expectEqual(@as(u32, 2), count);

            // First entry: current version (v2)
            var off: usize = 4;
            const v2_len = std.mem.readInt(u32, data[off..][0..4], .little);
            off += 4;
            try testing.expectEqualSlices(u8, "v2", data[off..][0..v2_len]);
            off += v2_len;
            const v2_ver = std.mem.readInt(u64, data[off..][0..8], .little);
            try testing.expectEqual(@as(u64, 2), v2_ver);
            off += 8;

            // Second entry: previous version (v1)
            const v1_len = std.mem.readInt(u32, data[off..][0..4], .little);
            off += 4;
            try testing.expectEqualSlices(u8, "v1", data[off..][0..v1_len]);
            off += v1_len;
            const v1_ver = std.mem.readInt(u64, data[off..][0..8], .little);
            try testing.expectEqual(@as(u64, 1), v1_ver);
        },
        else => return error.TestUnexpectedResult,
    }
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
