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

    pub fn init(allocator: Allocator, kv: *KVProjection) KVHandler {
        return .{
            .kv = kv,
            .allocator = allocator,
            .next_lsn = 1,
        };
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
                const result = CommandResult{ .kv_value = .{ .value = entry.value, .version = entry.lsn } };
                sendKVResponse(shard, conn, req.header.request_id, result);
                return;
            }
            // Key absent — register waiter in unified pool; response sent when key is created.
            _ = shard.waiter_pool.register(.{
                .kind = .kv_get,
                .fd = conn.fd,
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
            const current_version: u64 = if (shard.kv_handler.*.kv.get(qkey)) |entry| entry.lsn else 0;
            _ = shard.waiter_pool.register(.{
                .kind = .kv_get,
                .fd = conn.fd,
                .request_id = req.header.request_id,
                .key = qkey,
                .min_version = current_version,
                .timeout_ms = wms,
            });
            conn.response_deferred = true;
            return;
        }

        // Normal non-blocking GET.
        const cmd_result = shard.kv_handler.*.handleCommand(req);
        defer shard.kv_handler.*.freeResult(cmd_result);
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

        // Build CommandPayload and propose through Raft (uses qualified key)
        const propose_result = proposeKVEntry(shard, .kv_put, req, qkey) catch |err| {
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

        // Build response from the committed version (the propose index IS the version)
        const cmd_result = CommandResult{ .kv_put_ok = .{ .version = propose_result.index } };

        // Track namespace data for non-empty delete check
        shard.namespace_handler.markNamespaceHasData(req.namespace);

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
        if (shard.kv_handler.*.kv.get(qkey) == null) {
            sendKVResponse(shard, conn, req.header.request_id, .kv_not_found);
            return;
        }

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
        _: u32,
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
        for (S.key_buf[0..raw_count]) |key| {
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

    // ── Raft Propose ────────────────────────────────────────────────────

    /// Build a CommandPayload from the request, set entry flags (TTL, tombstone),
    /// and propose the entry through RaftNode. Returns the ProposeResult with
    /// .index (becomes the entry version) and .term.
    ///
    /// After this returns successfully, call `applyCommittedEntries()` to apply
    /// any newly committed entries to the KV projection.
    fn proposeKVEntry(shard: *Shard, entry_type: entry_mod.EntryType, req: Request, qualified_key: []const u8) !@import("../raft/node.zig").ProposeResult {
        // Build CommandPayload (namespace_hash:4 + key_len:2 + val_len:4 + key + value)
        var payload_buf: [MAX_ENTRY_PAYLOAD]u8 = undefined;
        const value = if (entry_type == .kv_delete) &[_]u8{} else req.value;
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

        const timestamp_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

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
        return .{ .kv_value = .{ .value = entry.value, .version = entry.lsn } };
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
                if (entry.lsn != expected_version) {
                    return .{ .kv_cas_failed = .{ .current_version = entry.lsn } };
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
        const timestamp = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
        const expiry_ns: u64 = if (req.getTtlSeconds()) |ttl_secs| blk: {
            if (ttl_secs == 0) break :blk 0;
            break :blk timestamp + ttl_secs * 1_000_000_000;
        } else 0;

        self.kv.put(qkey, req.value, lsn, 0, timestamp, expiry_ns) catch {
            return .{ .err = .{ .code = .internal_error, .message = "put failed" } };
        };
        return .{ .kv_put_ok = .{ .version = lsn } };
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

        const lsn = self.nextLsn();
        const timestamp = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
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
                if (entry.lsn != expected_version) {
                    return .{ .kv_cas_failed = .{ .current_version = entry.lsn } };
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
        _ = self;
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }

        // Version history not yet implemented in KVProjection MVCC
        return .{ .err = .{ .code = .invalid_request, .message = "history not yet implemented" } };
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
        .kv_put_ok => {
            shard.sendOkResponse(conn, request_id, "");
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
        .kv_txn_conflict => .conflict,
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
            .reserved = 0,
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

    // Verify pre-route hooks
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_get)] != null);
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_put)] != null);
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_scan)] == null); // walk-only, no pre-route
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_history)] == null); // no routing for history

    // 5 handlers registered
    try testing.expectEqual(@as(u16, 5), dispatcher.handler_count);
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

test "kv handler: history not implemented" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_history, "k", "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
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
