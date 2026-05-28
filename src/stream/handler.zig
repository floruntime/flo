//! Stream Handler — registers stream and consumer-group opcodes with Dispatcher.
//!
//! Read operations (read, info, group_pending) query the StreamProjection for
//! per-stream records, then read payloads directly from the UAL
//! (zero-copy when the entry is contiguous in the hot ring).
//!
//! Write operations (append) build a CommandEntry, persist it to the UAL via
//! Partition.apply(), then update the StreamProjection's per-stream state.
//! Raft propose will be added when the full consensus pipeline is connected.
//!
//! ## Design (UNIFIED_STORAGE_DESIGN.md — P6: Zero-Copy Stream Path)
//!
//! Stream data entries live in the UAL and are read directly — no materialization
//! step. The UAL entry IS the stream record. The StreamProjection maintains
//! per-stream StreamState (StreamID → ual_index) and PEL-based consumer groups.
//!
//! ## Opcode Ranges
//!
//!   Streams:        0x10–0x1F (append, read, trim, info, subscribe, list, create, alter)
//!   Consumer Groups: 0x20–0x2C (create, join, leave, read, ack, nack, claim, pending, touch, info, delete)
//!
//! ## Data Model
//!
//! The StreamProjection tracks per-stream StreamRecord (StreamID → ual_index).
//! Actual message payloads live in the UAL at the recorded ual_index.
//! Consumer groups use a Pending Entry List (PEL) for per-message ack tracking.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const stream_mod = @import("../projection/stream.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const router = @import("../node/router.zig");
const wire = @import("../util/wire.zig");
const ns_keys = @import("../namespace/handler.zig");
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

const CommandResult = result_mod.CommandResult;
const StreamProjection = stream_mod.StreamProjection;
const StreamID = stream_mod.StreamID;
const StreamRecord = stream_mod.StreamRecord;
const PendingEntry = stream_mod.PendingEntry;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const OptionsBuilder = proto.OptionsBuilder;
const WireReader = wire.WireReader;
const waiter_pool_mod = @import("../node/waiter_pool.zig");

// UAL storage integration
const partition_mod = @import("../storage/partition.zig");
const Partition = partition_mod.Partition;
const entry_mod = @import("../storage/ual/entry.zig");
const EntryType = entry_mod.EntryType;
const UAL = @import("../storage/ual/ual.zig").UAL;
const persistence_mod = @import("../storage/persistence.zig");
const ReplayRegistry = persistence_mod.ReplayRegistry;
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;

// ═══════════════════════════════════════════════════════════════════════════════
// StreamHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const StreamHandler = struct {
    stream: *StreamProjection,
    partition: *Partition,
    allocator: Allocator,

    /// Opaque pointer to owning Shard (avoids circular import).
    /// Set after init by Shard.init. Required for persistEntry() Raft writes.
    shard_ptr: ?*anyopaque,

    /// Global metrics registry (optional, set by runtime when dashboard is enabled).
    metrics_registry: ?*MetricsRegistry,

    /// Maximum number of messages in a single read response.
    const MAX_READ_BATCH: usize = 1000;
    const DEFAULT_READ_BATCH: usize = 100;

    pub fn init(allocator: Allocator, partition: *Partition) StreamHandler {
        return .{
            .stream = &partition.stream,
            .partition = partition,
            .allocator = allocator,
            .shard_ptr = null,
            .metrics_registry = null,
        };
    }

    pub fn deinit(self: *StreamHandler) void {
        _ = self;
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    /// Register stream and consumer-group opcode handlers with the Dispatcher.
    pub fn register(dispatcher: *Dispatcher) void {
        // Stream operations (0x10–0x1F)
        dispatcher.registerWithRoute(.stream_append, dispatchAppend, preRouteByStream);
        dispatcher.registerWithRoute(.stream_read, dispatchRead, preRouteByStream);
        dispatcher.registerWithRoute(.stream_trim, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_info, dispatchStream, preRouteByStream);
        dispatcher.registerWalk(.stream_list, dispatchStream, localScanStreams);
        dispatcher.registerWithRoute(.stream_create, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_alter, dispatchStream, preRouteByStream);

        // Consumer group operations (0x20–0x2C)
        dispatcher.registerWithRoute(.stream_group_create, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_join, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_leave, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_read, dispatchGroupRead, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_ack, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_nack, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_delete, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_info, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_pending, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_touch, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_claim, dispatchStream, preRouteByStream);
    }

    // ── Pre-Route Hooks ─────────────────────────────────────────────────

    /// Route by stream name — single-partition operations.
    /// Uses namespace-qualified hash: hash(namespace \0 stream_name)
    fn preRouteByStream(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    // ── Shard Walker: Local Scan ────────────────────────────────────────

    /// ShardWalker LocalScanFn for stream_list — scans unique stream
    /// names from one shard's StreamProjection.
    /// Filters by namespace prefix and strips prefix from results.
    pub fn localScanStreams(
        ctx: *anyopaque,
        namespace: []const u8,
        _: []const u8,
        _: ?[]const u8,
        _: u32,
    ) dispatcher_mod.NameWalker.ScanResult {
        const stream: *StreamProjection = @ptrCast(@alignCast(ctx));
        const S = struct {
            threadlocal var name_buf: [1024][]const u8 = undefined;
            threadlocal var ns_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        };

        // Build namespace prefix for filtering
        const ns_prefix = ns_keys.namespacePrefix(&S.ns_buf, namespace);

        // Scan all qualified names
        const raw_count = stream.scanStreamNames(&S.name_buf);

        // Filter by namespace and strip prefix
        var count: usize = 0;
        for (S.name_buf[0..raw_count]) |name| {
            if (ns_prefix.len == 0) {
                // Default namespace — only include bare names (no NUL separator)
                if (std.mem.indexOfScalar(u8, name, ns_keys.NAMESPACE_SEPARATOR) == null) {
                    S.name_buf[count] = name;
                    count += 1;
                }
            } else if (std.mem.startsWith(u8, name, ns_prefix)) {
                // Non-default namespace — strip prefix
                S.name_buf[count] = name[ns_prefix.len..];
                count += 1;
            }
        }

        return .{ .items = S.name_buf[0..count], .next_cursor = null };
    }

    // ── Dispatch Wrappers ───────────────────────────────────────────────

    fn dispatchStream(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.stream_handler.handleCommand(req);
        defer shard.stream_handler.freeResult(cmd_result);

        // Track namespace data for stream create operations
        const op: proto.OpCode = @enumFromInt(req.header.op_code);
        if (op == .stream_create) {
            switch (cmd_result) {
                .ok => shard.namespace_handler.markNamespaceHasData(req.namespace, shard),
                else => {},
            }
        }

        sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
    }

    /// Dedicated dispatch for stream_append — notifies blocking read waiters after append.
    fn dispatchAppend(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.stream_handler.handleCommand(req);
        defer shard.stream_handler.freeResult(cmd_result);

        // After a successful append, notify any blocking read waiters and track namespace data
        switch (cmd_result) {
            .stream_append_ok => {
                shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
                if (req.key.len > 0) {
                    shard.waiter_pool.notify(.stream_read, req.key, @import("../node/shard.zig").resolveStreamWaiter, @ptrCast(shard));
                    shard.waiter_pool.notify(.stream_group_read, req.key, @import("../node/shard.zig").resolveGroupReadWaiter, @ptrCast(shard));
                }
            },
            else => {},
        }

        sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
    }

    /// Dedicated dispatch for stream_read — supports blocking via block_ms.
    fn dispatchRead(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        // Check for blocking read (--follow / --wait → block_ms)
        const block_ms = req.getBlockMs();

        if (block_ms) |bms| {
            // Try reading first — if data exists, return immediately
            const cmd_result = shard.stream_handler.handleCommand(req);

            switch (cmd_result) {
                .stream_messages => |m| {
                    // Check if we got any actual messages (count > 0)
                    if (m.data.len > 4) {
                        const count = std.mem.readInt(u32, m.data[0..4], .little);
                        if (count > 0) {
                            defer shard.stream_handler.freeResult(cmd_result);
                            sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
                            return;
                        }
                    }
                    // No data — register a blocking waiter
                    shard.stream_handler.freeResult(cmd_result);
                },
                else => {
                    // Error or other result — send immediately
                    defer shard.stream_handler.freeResult(cmd_result);
                    sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
                    return;
                },
            }

            // Register waiter with UAL max index as version (monotonically increasing)
            const registered = shard.waiter_pool.register(.{
                .kind = .stream_read,
                .fd = conn.fd,
                .owner_shard = conn.owner_shard,
                .conn_id = conn.id,
                .request_id = req.header.request_id,
                .key = req.key,
                .min_version = shard.defaultPartition().ual.max_index,
                .timeout_ms = bms,
            });
            if (!registered) {
                // Pool full — send an empty result now rather than deferring a
                // response that has no waiter to ever complete it.
                shard.sendOkResponse(conn, req.header.request_id, "");
                return;
            }
            conn.response_deferred = true;
            return;
        }

        // Non-blocking read — standard path
        const cmd_result = shard.stream_handler.handleCommand(req);
        defer shard.stream_handler.freeResult(cmd_result);
        sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
    }

    /// Dedicated dispatch for stream_group_read — supports blocking via block_ms.
    ///
    /// Mirrors `dispatchRead` but for consumer group reads.  When no messages
    /// are available and `block_ms` is set, registers a waiter that gets woken
    /// when new data is appended to the stream (see `dispatchAppend`).
    fn dispatchGroupRead(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        const block_ms = req.getBlockMs();

        if (block_ms) |bms| {
            // Try reading first — if data exists, return immediately
            const cmd_result = shard.stream_handler.handleCommand(req);

            switch (cmd_result) {
                .group_messages => |m| {
                    // Check if we got any actual messages (count > 0)
                    if (m.data.len > 4) {
                        const count = std.mem.readInt(u32, m.data[0..4], .little);
                        if (count > 0) {
                            defer shard.stream_handler.freeResult(cmd_result);
                            sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
                            return;
                        }
                    }
                    // No data — register a blocking waiter
                    shard.stream_handler.freeResult(cmd_result);
                },
                else => {
                    // Error or other result — send immediately
                    defer shard.stream_handler.freeResult(cmd_result);
                    sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
                    return;
                },
            }

            // Register waiter — woken by dispatchAppend or expired by timeout
            const is_pattern = req.key.len > 0 and req.key[req.key.len - 1] == '*';
            const registered = shard.waiter_pool.register(.{
                .kind = .stream_group_read,
                .fd = conn.fd,
                .owner_shard = conn.owner_shard,
                .conn_id = conn.id,
                .request_id = req.header.request_id,
                .key = if (is_pattern) req.key[0 .. req.key.len - 1] else req.key,
                .min_version = shard.defaultPartition().ual.max_index,
                .timeout_ms = bms,
                .pattern = is_pattern,
            });
            if (!registered) {
                // Pool full — send an empty result now rather than deferring a
                // response that has no waiter to ever complete it.
                shard.sendOkResponse(conn, req.header.request_id, "");
                return;
            }
            conn.response_deferred = true;
            return;
        }

        // Non-blocking group read — standard path
        const cmd_result = shard.stream_handler.handleCommand(req);
        defer shard.stream_handler.freeResult(cmd_result);
        sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    /// Dispatch a stream command to the appropriate handler.
    pub fn handleCommand(self: *StreamHandler, req: Request) CommandResult {
        const op: OpCode = @enumFromInt(req.header.op_code);
        return switch (op) {
            // Stream operations
            .stream_append => self.handleAppend(req),
            .stream_read => self.handleRead(req),
            .stream_trim => self.handleTrim(req),
            .stream_info => self.handleInfo(req),
            .stream_list => self.handleList(req),
            .stream_create => self.handleCreate(req),
            .stream_alter => self.handleAlter(req),

            // Consumer group operations
            .stream_group_create => self.handleGroupCreate(req),
            .stream_group_join => self.handleGroupJoin(req),
            .stream_group_leave => self.handleGroupLeave(req),
            .stream_group_read => self.handleGroupRead(req),
            .stream_group_ack => self.handleGroupAck(req),
            .stream_group_nack => self.handleGroupNack(req),
            .stream_group_delete => self.handleGroupDelete(req),
            .stream_group_info => self.handleGroupInfo(req),
            .stream_group_pending => self.handleGroupPending(req),
            .stream_group_touch => self.handleGroupTouch(req),
            .stream_group_claim => self.handleGroupClaim(req),

            else => .{ .err = .{ .code = .invalid_request, .message = "unknown stream opcode" } },
        };
    }

    // ── APPEND ──────────────────────────────────────────────────────────

    fn handleAppend(self: *StreamHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "stream name is required" } };
        }

        // Resolve partition index from options
        var partition_index: u32 = 0;
        if (req.findOption(.partition)) |opt| {
            if (opt.asU32()) |p| {
                partition_index = p;
            }
        } else if (req.findOption(.partition_key)) |opt| {
            // Hash the partition key and map to a partition index
            const pk_bytes = opt.data;
            if (pk_bytes.len > 0) {
                const pc = self.stream.getPartitionCount(req.key);
                partition_index = @intCast(std.hash.Wyhash.hash(0, pk_bytes) % pc);
            }
        }

        const payload_value = if (req.value.len > 0) req.value else "";

        // Persist through Raft, then apply the committed entry (same path as segment replay).
        const sptr = self.shard_ptr orelse {
            return .{ .err = .{ .code = .internal_error, .message = "shard not wired" } };
        };
        const shard = shardFromPtr(sptr);
        const raft_index = persistence_mod.persistEntry(shard, .stream_append, entry_mod.Flags.NONE, req.namespace, req.key, payload_value) catch {
            return .{ .err = .{ .code = .internal_error, .message = "raft persist failed" } };
        };

        const stream_id = self.applyCommittedStreamEntries(shard, raft_index, partition_index) catch {
            return .{ .err = .{ .code = .internal_error, .message = "stream apply failed" } };
        };

        // Register stream name for listing (namespace-qualified)
        var ns_reg_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const ns_stream_name = ns_keys.qualifyKey(&ns_reg_buf, req.namespace, req.key) catch req.key;
        self.stream.registerStream(ns_stream_name) catch {};

        // Register in global metrics registry for dashboard/Prometheus
        if (self.metrics_registry) |mr| {
            _ = mr.registerStream(req.namespace, req.key, 0) catch {};
        }

        return .{ .stream_append_ok = .{
            .sequence = stream_id.sequence,
            .timestamp_ms = @as(i64, @intCast(stream_id.timestamp_ms)),
        } };
    }

    // ── READ ────────────────────────────────────────────────────────────

    fn handleRead(self: *StreamHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "stream name is required" } };
        }

        const limit = req.getLimit() orelse DEFAULT_READ_BATCH;
        const capped = @min(limit, MAX_READ_BATCH);

        const ns_hash = router.namespaceHash(req.namespace);
        const name_hash = router.nameHash(ns_hash, req.key);

        // Determine start ID
        var start_id = StreamID.MIN;

        if (req.findOption(.stream_tail) != null) {
            start_id = self.stream.streamLastId(name_hash);
        } else if (req.findOption(.stream_start)) |opt| {
            if (opt.asStreamId()) |sid| {
                if (sid.timestamp_ms > 0) {
                    start_id = .{ .timestamp_ms = sid.timestamp_ms, .sequence = sid.sequence };
                } else if (sid.sequence > 0) {
                    // Bare sequence — convert to StreamID for lookup
                    start_id = StreamID.fromSeq(sid.sequence);
                }
            }
        }

        // Determine end ID (for range reads)
        var end_id = StreamID.MAX;
        if (req.findOption(.stream_end)) |opt| {
            if (opt.asStreamId()) |sid| {
                if (sid.timestamp_ms > 0) {
                    end_id = .{ .timestamp_ms = sid.timestamp_ms, .sequence = sid.sequence };
                }
            }
        }

        // Parse optional partition filter
        var partition_filter: ?u32 = null;
        if (req.findOption(.partition)) |opt| {
            partition_filter = opt.asU32();
        } else if (req.findOption(.partition_key)) |opt| {
            const pk_bytes = opt.data;
            if (pk_bytes.len > 0) {
                const pc = self.stream.getPartitionCount(req.key);
                partition_filter = @intCast(std.hash.Wyhash.hash(0, pk_bytes) % pc);
            }
        }

        // StreamID-based read path
        var buf: [MAX_READ_BATCH]StreamRecord = undefined;
        const count = if (end_id.eql(StreamID.MAX))
            self.stream.readStreamAfter(name_hash, start_id, partition_filter, buf[0..capped])
        else
            self.stream.readStreamRange(name_hash, start_id, end_id, partition_filter, buf[0..capped]);

        const data = self.serializeStreamRecordsWithPayloads(buf[0..count], req.key) catch {
            return .{ .err = .{ .code = .internal_error, .message = "read serialization failed" } };
        };

        const last_id = if (count > 0) buf[count - 1].id else StreamID.MIN;
        return .{ .stream_messages = .{
            .data = data,
            .next_timestamp_ms = last_id.timestamp_ms,
            .next_sequence = last_id.sequence,
        } };
    }

    // ── TRIM ────────────────────────────────────────────────────────────

    fn handleTrim(self: *StreamHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "stream name is required" } };
        }

        const ns_hash = router.namespaceHash(req.namespace);
        const name_hash = router.nameHash(ns_hash, req.key);

        // Parse trim target as StreamID
        var trim_id = StreamID.MIN;

        if (req.findOption(.stream_end)) |opt| {
            if (opt.asStreamId()) |sid| {
                if (sid.timestamp_ms > 0) {
                    trim_id = .{ .timestamp_ms = sid.timestamp_ms, .sequence = sid.sequence };
                } else if (sid.sequence > 0) {
                    trim_id = StreamID.fromSeq(sid.sequence);
                }
            }
        }

        if (trim_id.eql(StreamID.MIN) and req.value.len > 0) {
            // Bare integer: trim the first N records — resolve to a StreamID
            const count = std.fmt.parseInt(u64, req.value, 10) catch 0;
            if (count > 0) {
                trim_id = self.stream.resolveNthRecordId(name_hash, count);
                if (trim_id.eql(StreamID.MIN)) {
                    return .{ .stream_trimmed = .{ .deleted_count = 0, .first_seq = 0 } };
                }
            }
        }

        if (trim_id.eql(StreamID.MIN)) {
            return .{ .err = .{ .code = .invalid_request, .message = "trim offset is required" } };
        }

        // Persist through Raft for durability and replication
        const deleted = self.persistAndApplyTrim(req.namespace, name_hash, trim_id);

        const first_id = self.stream.streamFirstId(name_hash);
        const first_seq = if (!first_id.eql(StreamID.MIN)) first_id.sequence else 0;

        return .{ .stream_trimmed = .{
            .deleted_count = deleted,
            .first_seq = first_seq,
        } };
    }

    // ── INFO ────────────────────────────────────────────────────────────

    fn handleInfo(self: *StreamHandler, req: Request) CommandResult {
        const ns_hash = router.namespaceHash(req.namespace);
        const name_hash = router.nameHash(ns_hash, req.key);
        const pc = self.stream.getPartitionCount(req.key);

        const first_id = self.stream.streamFirstId(name_hash);
        const last_id = self.stream.streamLastId(name_hash);
        const count = self.stream.streamRecordCount(name_hash);

        const meta = self.stream.stream_metadata.get(req.key);

        return .{ .stream_info = .{
            .first_timestamp_ms = if (count > 0) first_id.timestamp_ms else 0,
            .first_seq = if (count > 0) first_id.sequence else 0,
            .last_timestamp_ms = if (count > 0) last_id.timestamp_ms else 0,
            .last_seq = if (count > 0) last_id.sequence else 0,
            .count = count,
            .bytes = 0,
            .partition_count = pc,
            .retention_age_s = if (meta) |m| m.retention_age_s else 0,
            .retention_count = if (meta) |m| m.retention_count else 0,
            .retention_bytes = if (meta) |m| m.retention_bytes else 0,
        } };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *StreamHandler, req: Request) CommandResult {
        // Single-shard fallback — ShardWalker handles the cross-shard case.
        // Build a namespace-filtered name-list response.
        var name_buf: [1024][]const u8 = undefined;
        const raw_count = self.stream.scanStreamNames(&name_buf);

        // Filter by namespace prefix and strip
        var ns_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const ns_prefix = ns_keys.namespacePrefix(&ns_buf, req.namespace);

        var filtered: [1024][]const u8 = undefined;
        var filtered_count: usize = 0;
        for (name_buf[0..raw_count]) |name| {
            if (ns_prefix.len == 0) {
                // Default namespace — only bare names (no NUL separator)
                if (std.mem.indexOfScalar(u8, name, ns_keys.NAMESPACE_SEPARATOR) == null) {
                    filtered[filtered_count] = name;
                    filtered_count += 1;
                }
            } else if (std.mem.startsWith(u8, name, ns_prefix)) {
                filtered[filtered_count] = name[ns_prefix.len..];
                filtered_count += 1;
            }
        }

        const data = serializeNameList(self.allocator, filtered[0..filtered_count], self.stream) catch {
            return .{ .err = .{ .code = .internal_error, .message = "list serialization failed" } };
        };

        return .{ .group_pending = .{ .data = data } };
    }

    // ── CREATE ──────────────────────────────────────────────────────────

    fn handleCreate(self: *StreamHandler, req: Request) CommandResult {
        // Register the stream name for listing (namespace-qualified)
        if (req.key.len > 0) {
            var ns_reg_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
            const ns_stream_name = ns_keys.qualifyKey(&ns_reg_buf, req.namespace, req.key) catch req.key;
            self.stream.registerStream(ns_stream_name) catch {};

            // Parse partition_count from wire value (u32 LE) if present
            var partition_count: u32 = 1;
            if (req.value.len >= 4) {
                var reader = WireReader.init(req.value);
                if (reader.readU32()) |pc| {
                    if (pc > 0) partition_count = pc;
                }
            }

            // Parse retention options from TLV
            const retention = parseRetentionOptions(req);

            // Compute name_hash for background trim lookups
            const ns_hash = router.namespaceHash(req.namespace);
            const name_hash = router.nameHash(ns_hash, req.key);

            self.stream.registerStreamMetadata(req.key, .{
                .partition_count = partition_count,
                .name_hash = name_hash,
                .retention_age_s = retention.age_s,
                .retention_count = retention.count,
                .retention_bytes = retention.bytes,
            }) catch {};

            // Register in global metrics registry for dashboard/Prometheus
            if (self.metrics_registry) |mr| {
                _ = mr.registerStream(req.namespace, req.key, 0) catch {};
            }
        }
        return .ok;
    }

    // ── ALTER ───────────────────────────────────────────────────────────

    fn handleAlter(self: *StreamHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "stream name is required" } };
        }

        // Check stream exists
        const existing = self.stream.stream_metadata.get(req.key) orelse {
            return .{ .err = .{ .code = .not_found, .message = "stream not found" } };
        };

        // Parse retention options from TLV
        const retention = parseRetentionOptions(req);

        // Merge: keep existing partition_count and name_hash, update retention
        self.stream.registerStreamMetadata(req.key, .{
            .partition_count = existing.partition_count,
            .name_hash = existing.name_hash,
            .retention_age_s = if (retention.age_s > 0) retention.age_s else existing.retention_age_s,
            .retention_count = if (retention.count > 0) retention.count else existing.retention_count,
            .retention_bytes = if (retention.bytes > 0) retention.bytes else existing.retention_bytes,
        }) catch {};

        return .ok;
    }

    /// Parse retention TLV options from a request.
    fn parseRetentionOptions(req: Request) struct { age_s: u64, count: u64, bytes: u64 } {
        var age_s: u64 = 0;
        var count: u64 = 0;
        var bytes: u64 = 0;

        var iter = req.getOptionsIterator();
        while (iter.next()) |opt| {
            switch (opt.tag) {
                .retention_age => {
                    if (opt.asU64()) |v| age_s = v;
                },
                .retention_count => {
                    if (opt.asU64()) |v| count = v;
                },
                .retention_bytes => {
                    if (opt.asU64()) |v| bytes = v;
                },
                else => {},
            }
        }

        return .{ .age_s = age_s, .count = count, .bytes = bytes };
    }

    // ── GROUP CREATE ────────────────────────────────────────────────────

    fn handleGroupCreate(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group] (binary length-prefixed)
        // Fallback: raw value as group name (for unit tests)
        var wire_format = false;
        const raw_group = blk: {
            if (req.value.len >= 2) {
                var reader = WireReader.init(req.value);
                if (reader.readLengthPrefixed(u16)) |name| {
                    if (name.len > 0) {
                        wire_format = true;
                        break :blk name;
                    }
                }
            }
            // Fallback: treat raw value as group name
            if (req.value.len > 0) break :blk req.value;
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

        // Namespace-qualify for isolation (wire format only)
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, raw_group, wire_format);

        const now_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;

        self.stream.createGroup(group_name, now_ns) catch |err| {
            return switch (err) {
                error.AlreadyExists => .{ .err = .{ .code = .already_exists, .message = "consumer group already exists" } },
                else => .{ .err = .{ .code = .internal_error, .message = "group creation failed" } },
            };
        };

        return .ok;
    }

    // ── GROUP DELETE ────────────────────────────────────────────────────

    fn handleGroupDelete(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group]
        const decoded = decodeGroupName(req.value) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, decoded.name, decoded.wire);

        if (self.stream.deleteGroup(group_name)) {
            return .ok;
        } else {
            return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
        }
    }

    // ── GROUP JOIN ──────────────────────────────────────────────────────

    fn handleGroupJoin(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group][consumer_len:u16][consumer]
        const pair = decodeGroupConsumer(req) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, pair.group, pair.wire);
        const member_id = if (pair.consumer.len > 0) pair.consumer else "default";
        const now_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;

        // Auto-create group if it doesn't exist
        self.stream.createGroup(group_name, now_ns) catch |err| {
            if (err != error.AlreadyExists) {
                return .{ .err = .{ .code = .internal_error, .message = "group creation failed" } };
            }
        };

        _ = self.stream.joinGroup(group_name, member_id, now_ns) catch |err| {
            return switch (err) {
                error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
                else => .{ .err = .{ .code = .internal_error, .message = "group join failed" } },
            };
        };

        return .{ .group_joined = .{
            .generation_id = 1, // Placeholder — real gen ID from rebalance
            .assigned_partitions = &[_]u32{0}, // Single partition for now
        } };
    }

    // ── GROUP LEAVE ─────────────────────────────────────────────────────

    fn handleGroupLeave(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group][consumer_len:u16][consumer]
        const pair = decodeGroupConsumer(req) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, pair.group, pair.wire);
        const member_id = if (pair.consumer.len > 0) pair.consumer else "default";

        _ = self.stream.leaveGroup(group_name, member_id) catch |err| {
            return switch (err) {
                error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
            };
        };

        return .ok;
    }

    // ── GROUP READ ──────────────────────────────────────────────────────

    fn handleGroupRead(self: *StreamHandler, req: Request) CommandResult {
        // Pattern group read: key ends with `*` (e.g. `events.*` or `*`)
        if (req.key.len > 0 and req.key[req.key.len - 1] == '*') {
            return self.handlePatternGroupRead(req);
        }

        // Wire format: [group_len:u16][group][consumer_len:u16][consumer]
        const pair = decodeGroupConsumer(req) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, pair.group, pair.wire);
        const consumer_id = if (pair.consumer.len > 0) pair.consumer else "default";

        // Auto-create group and join consumer if not exists
        const now_ms: u64 = @intCast(@import("stdx").time.milliTimestamp());
        const now_ns = now_ms * 1_000_000;
        self.stream.createGroup(group_name, now_ns) catch |err| {
            if (err != error.AlreadyExists) {
                return .{ .err = .{ .code = .internal_error, .message = "group creation failed" } };
            }
        };
        _ = self.stream.joinGroup(group_name, consumer_id, now_ns) catch {};

        const limit = req.getLimit() orelse DEFAULT_READ_BATCH;
        const capped = @min(limit, MAX_READ_BATCH);

        const ns_hash = router.namespaceHash(req.namespace);
        const name_hash = router.nameHash(ns_hash, req.key);

        // Deliver via PEL — reads after group's last_delivered_id
        var buf: [MAX_READ_BATCH]StreamRecord = undefined;
        const count = self.stream.groupDeliver(group_name, name_hash, consumer_id, capped, now_ms, buf[0..capped]) catch |err| {
            return switch (err) {
                error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
                else => .{ .err = .{ .code = .internal_error, .message = "group deliver failed" } },
            };
        };

        const data = self.serializeStreamRecordsWithPayloads(buf[0..count], req.key) catch {
            return .{ .err = .{ .code = .internal_error, .message = "group read serialization failed" } };
        };

        return .{ .group_messages = .{ .data = data } };
    }

    /// Pattern group read — reads from all streams matching a prefix pattern.
    ///
    /// The key `events.*` matches all streams starting with `events.`.
    /// A bare `*` matches all streams in the namespace.
    /// Records from each matching stream carry the stream name as key identity.
    fn handlePatternGroupRead(self: *StreamHandler, req: Request) CommandResult {
        const pair = decodeGroupConsumer(req) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, pair.group, pair.wire);
        const consumer_id = if (pair.consumer.len > 0) pair.consumer else "default";

        const now_ms: u64 = @intCast(@import("stdx").time.milliTimestamp());
        const now_ns = now_ms * 1_000_000;
        self.stream.createGroup(group_name, now_ns) catch |err| {
            if (err != error.AlreadyExists) {
                return .{ .err = .{ .code = .internal_error, .message = "group creation failed" } };
            }
        };
        _ = self.stream.joinGroup(group_name, consumer_id, now_ns) catch {};

        const limit = req.getLimit() orelse DEFAULT_READ_BATCH;
        const capped = @min(limit, MAX_READ_BATCH);

        // Strip trailing `*` to get the bare prefix (e.g. `events.*` → `events.`, `*` → ``)
        const bare_prefix = req.key[0 .. req.key.len - 1];

        // Build namespace-qualified prefix for filtering stream_names
        var ns_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const ns_prefix = ns_keys.namespacePrefix(&ns_buf, req.namespace);

        // Scan all stream names from this shard's projection
        var name_buf: [1024][]const u8 = undefined;
        const raw_count = self.stream.scanStreamNames(&name_buf);

        const ns_hash = router.namespaceHash(req.namespace);

        // Collect records from all matching streams
        var all_buf: [MAX_READ_BATCH]StreamRecord = undefined;
        // Track which stream each batch of records came from (for serialization)
        var stream_names_out: [MAX_READ_BATCH][]const u8 = undefined;
        var total: usize = 0;

        for (name_buf[0..raw_count]) |qualified_name| {
            if (total >= capped) break;

            // Filter by namespace
            const display_name = blk: {
                if (ns_prefix.len == 0) {
                    // Default namespace — only bare names (no NUL separator)
                    if (std.mem.indexOfScalar(u8, qualified_name, ns_keys.NAMESPACE_SEPARATOR) != null) continue;
                    break :blk qualified_name;
                } else {
                    if (!std.mem.startsWith(u8, qualified_name, ns_prefix)) continue;
                    break :blk qualified_name[ns_prefix.len..];
                }
            };

            // Filter by pattern prefix
            if (bare_prefix.len > 0 and !std.mem.startsWith(u8, display_name, bare_prefix)) continue;

            // Compute name_hash for this stream
            const name_hash = router.nameHash(ns_hash, display_name);

            const remaining = capped - total;
            const count = self.stream.groupDeliver(group_name, name_hash, consumer_id, remaining, now_ms, all_buf[total .. total + remaining]) catch continue;

            // Tag each record with the stream name for serialization
            for (total..total + count) |i| {
                stream_names_out[i] = display_name;
            }
            total += count;
        }

        // Serialize combined results with per-record stream names
        const data = self.serializePatternGroupRecords(all_buf[0..total], stream_names_out[0..total]) catch {
            return .{ .err = .{ .code = .internal_error, .message = "pattern group read serialization failed" } };
        };

        return .{ .group_messages = .{ .data = data } };
    }

    // ── GROUP ACK ───────────────────────────────────────────────────────

    fn handleGroupAck(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][timestamp_ms:u64][sequence:u64]*
        var reader = WireReader.init(req.value);
        const raw_group = reader.readLengthPrefixed(u16) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        _ = reader.readLengthPrefixed(u16); // consumer

        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, raw_group, true);

        // Read StreamID array
        var ids: [MAX_READ_BATCH]StreamID = undefined;
        var id_count: usize = 0;
        if (reader.readU32()) |count| {
            for (0..count) |_| {
                const ts = reader.readU64() orelse break;
                const seq = reader.readU64() orelse break;
                if (id_count < MAX_READ_BATCH) {
                    ids[id_count] = .{ .timestamp_ms = ts, .sequence = seq };
                    id_count += 1;
                }
            }
        }

        if (id_count == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "stream IDs required for ack" } };
        }

        if (group_name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        }

        _ = self.stream.groupAck(group_name, ids[0..id_count]) catch |err| {
            return switch (err) {
                error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
            };
        };

        return .ok;
    }

    // ── GROUP NACK ──────────────────────────────────────────────────────

    fn handleGroupNack(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][timestamp_ms:u64][sequence:u64]*
        var reader = WireReader.init(req.value);
        const raw_group = reader.readLengthPrefixed(u16) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        _ = reader.readLengthPrefixed(u16); // consumer

        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, raw_group, true);

        // Read StreamIDs to NACK
        var ids: [MAX_READ_BATCH]StreamID = undefined;
        var id_count: usize = 0;
        if (reader.readU32()) |count| {
            for (0..count) |_| {
                const ts = reader.readU64() orelse break;
                const seq = reader.readU64() orelse break;
                if (id_count < MAX_READ_BATCH) {
                    ids[id_count] = .{ .timestamp_ms = ts, .sequence = seq };
                    id_count += 1;
                }
            }
        }

        if (group_name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        }

        const now_ms: u64 = @intCast(@import("stdx").time.milliTimestamp());
        if (id_count > 0) {
            _ = self.stream.groupNack(group_name, ids[0..id_count], now_ms) catch |err| {
                return switch (err) {
                    error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
                };
            };
        } else {
            if (self.stream.getGroup(group_name) == null) {
                return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
            }
        }

        return .ok;
    }

    // ── GROUP INFO ──────────────────────────────────────────────────────

    fn handleGroupInfo(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group] (same as pending/delete)
        const decoded = decodeGroupName(req.value) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, decoded.name, decoded.wire);

        const group = self.stream.getGroup(group_name) orelse {
            return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
        };

        // Serialize group info with PEL count
        const data = serializeGroupInfo(self.allocator, group) catch {
            return .{ .err = .{ .code = .internal_error, .message = "group info serialization failed" } };
        };

        return .{ .group_pending = .{ .data = data } };
    }

    // ── GROUP PENDING ───────────────────────────────────────────────────

    fn handleGroupPending(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group]
        const decoded = decodeGroupName(req.value) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, decoded.name, decoded.wire);

        // Get actual PEL entries
        var pel_buf: [MAX_READ_BATCH]PendingEntry = undefined;
        const count = self.stream.groupPending(group_name, null, &pel_buf) catch |err| {
            return switch (err) {
                error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
            };
        };

        // Serialize PEL entries
        // Wire format: [count:u32]([timestamp_ms:u64][sequence:u64][delivery_count:u32][consumer_len:u16][consumer])* 
        const data = serializePendingEntries(self.allocator, pel_buf[0..count]) catch {
            return .{ .err = .{ .code = .internal_error, .message = "pending serialization failed" } };
        };

        return .{ .group_pending = .{ .data = data } };
    }

    // ── GROUP CLAIM ─────────────────────────────────────────────────────

    /// Cursor-based PEL claim (FLO-102). Scans the group's pending entry list
    /// from `start_id` in StreamID order, claims up to `count` entries whose
    /// idle time ≥ `min_idle_ms` for `consumer`, and returns the full records
    /// (payload + headers) plus a trailing 16-byte next-cursor.
    ///
    /// Wire (value):
    ///   [group_len:u16][group]
    ///   [consumer_len:u16][consumer]
    ///   [min_idle_ms:u32]
    ///   [start_id_ts:u64][start_id_seq:u64]
    ///   [count:u32]
    ///
    /// Response (group_messages.data):
    ///   <serializeStreamRecordsWithPayloads blob>
    ///   [next_cursor_ts:u64][next_cursor_seq:u64]   ← 16-byte trailer
    ///
    /// `next_cursor == StreamID.MAX (max,max)` means the PEL has been fully
    /// scanned. The SDK loops, feeding next_cursor back as start_id, until the
    /// response carries zero records or the MAX sentinel.
    ///
    /// Drain-own-pending (reconnect) is `consumer = me`, `min_idle_ms = 0`,
    /// `start_id = MIN`. Steal-from-idle (rebalance) is `min_idle_ms > 0`.
    fn handleGroupClaim(self: *StreamHandler, req: Request) CommandResult {
        var reader = WireReader.init(req.value);
        const raw_group = reader.readLengthPrefixed(u16) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        const consumer_raw = reader.readLengthPrefixed(u16) orelse "";
        const min_idle_ms: u64 = reader.readU32() orelse 0;
        const start_ts = reader.readU64() orelse 0;
        const start_seq = reader.readU64() orelse 0;
        const count: u32 = reader.readU32() orelse DEFAULT_READ_BATCH;

        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, raw_group, true);
        const consumer_id = if (consumer_raw.len > 0) consumer_raw else "default";

        const start_id = StreamID{ .timestamp_ms = start_ts, .sequence = start_seq };
        const capped = @min(@as(usize, count), MAX_READ_BATCH);
        const now_ms: u64 = @intCast(@import("stdx").time.milliTimestamp());

        // Claim a page of PEL entries → IDs + next cursor.
        var id_buf: [MAX_READ_BATCH]StreamID = undefined;
        const claim_res = self.stream.groupAutoclaim(group_name, consumer_id, min_idle_ms, now_ms, start_id, capped, id_buf[0..capped]) catch |err| {
            return switch (err) {
                error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
            };
        };

        // Resolve claimed IDs → full records (payload + headers) in one hop.
        const ns_hash = router.namespaceHash(req.namespace);
        const name_hash = router.nameHash(ns_hash, req.key);
        var rec_buf: [MAX_READ_BATCH]StreamRecord = undefined;
        const rec_count = self.stream.readStreamByIds(name_hash, id_buf[0..claim_res.count], rec_buf[0..claim_res.count]);

        const records_blob = self.serializeStreamRecordsWithPayloads(rec_buf[0..rec_count], req.key) catch {
            return .{ .err = .{ .code = .internal_error, .message = "claim serialization failed" } };
        };

        // Append the 16-byte next-cursor to the records blob.
        const data = self.allocator.alloc(u8, records_blob.len + 16) catch {
            self.allocator.free(records_blob);
            return .{ .err = .{ .code = .internal_error, .message = "claim alloc failed" } };
        };
        @memcpy(data[0..records_blob.len], records_blob);
        self.allocator.free(records_blob);
        std.mem.writeInt(u64, data[records_blob.len..][0..8], claim_res.next_cursor.timestamp_ms, .little);
        std.mem.writeInt(u64, data[records_blob.len + 8 ..][0..8], claim_res.next_cursor.sequence, .little);

        return .{ .group_messages = .{ .data = data } };
    }

    // ── GROUP TOUCH ─────────────────────────────────────────────────────

    fn handleGroupTouch(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][timestamp_ms:u64][sequence:u64]*
        var reader = WireReader.init(req.value);
        const raw_group = reader.readLengthPrefixed(u16) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, raw_group, true);
        _ = reader.readLengthPrefixed(u16); // consumer

        // Read StreamIDs to touch
        var ids: [MAX_READ_BATCH]StreamID = undefined;
        var id_count: usize = 0;
        if (reader.readU32()) |count| {
            for (0..count) |_| {
                const ts = reader.readU64() orelse break;
                const seq = reader.readU64() orelse break;
                if (id_count < MAX_READ_BATCH) {
                    ids[id_count] = .{ .timestamp_ms = ts, .sequence = seq };
                    id_count += 1;
                }
            }
        }

        const now_ms: u64 = @intCast(@import("stdx").time.milliTimestamp());
        var touched_count: u32 = 0;
        if (id_count > 0) {
            touched_count = self.stream.groupTouch(group_name, ids[0..id_count], now_ms) catch |err| {
                return switch (err) {
                    error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
                };
            };
        } else {
            if (self.stream.getGroup(group_name) == null) {
                return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
            }
        }

        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, touched_count, .little);
        const data = self.allocator.alloc(u8, 4) catch {
            return .{ .err = .{ .code = .internal_error, .message = "alloc failed" } };
        };
        @memcpy(data, &buf);
        return .{ .group_pending = .{ .data = data } };
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    /// A resolved record ready for wire serialization.
    /// Produced by expanding batch payloads from the UAL.
    const ResolvedRecord = struct {
        sequence: u64,
        timestamp_ms: u64,
        partition_index: u32,
        tier: u8,
        payload: []const u8,
        /// Raw stored header bytes: [key_len:u16][key][val_len:u16][val]...
        headers_raw: []const u8,
        header_count: u32,
    };

    /// Expand StreamRecords into resolved records by unpacking batch payloads.
    /// One StreamRecord may expand into N records if it contains a batch blob.
    fn expandRecords(self: *StreamHandler, records: []const StreamRecord, out: []ResolvedRecord) usize {
        var count: usize = 0;
        for (records) |rec| {
            if (count >= out.len) break;
            const result = self.getPayloadAndTier(rec.ual_index);
            var batch_buf: [100]UnpackedRecord = undefined;
            const batch_n = unpackBatch(result.payload, &batch_buf);

            for (batch_buf[0..batch_n], 0..) |br, j| {
                if (count >= out.len) break;
                out[count] = .{
                    .sequence = rec.id.sequence + j,
                    .timestamp_ms = rec.id.timestamp_ms,
                    .partition_index = rec.partition_index,
                    .tier = result.tier,
                    .payload = br.payload,
                    .headers_raw = br.headers_raw,
                    .header_count = br.header_count,
                };
                count += 1;
            }
        }
        return count;
    }

    /// Compute re-serialized header size: stored format uses u16 lengths,
    /// response format uses u32 — each header gains 4 bytes.
    fn responseHeaderSize(hdr_count: u32, headers_raw_len: usize) usize {
        return headers_raw_len + (hdr_count * 4);
    }

    /// Write re-serialized headers into buf at pos. Widens u16→u32 length fields.
    /// Returns new pos after writing.
    fn writeResponseHeaders(headers_raw: []const u8, hdr_count: u32, buf: []u8, start_pos: usize) usize {
        var in_pos: usize = 0;
        var pos = start_pos;
        var h: u32 = 0;
        while (h < hdr_count) : (h += 1) {
            // key_len: u16 → u32
            const key_len = std.mem.readInt(u16, headers_raw[in_pos..][0..2], .little);
            in_pos += 2;
            std.mem.writeInt(u32, buf[pos..][0..4], key_len, .little);
            pos += 4;
            @memcpy(buf[pos .. pos + key_len], headers_raw[in_pos .. in_pos + key_len]);
            in_pos += key_len;
            pos += key_len;
            // val_len: u16 → u32
            const val_len = std.mem.readInt(u16, headers_raw[in_pos..][0..2], .little);
            in_pos += 2;
            std.mem.writeInt(u32, buf[pos..][0..4], val_len, .little);
            pos += 4;
            @memcpy(buf[pos .. pos + val_len], headers_raw[in_pos .. in_pos + val_len]);
            in_pos += val_len;
            pos += val_len;
        }
        return pos;
    }

    /// Serialize StreamRecord entries with full wire format including payloads and headers.
    ///
    /// Wire format per record:
    ///   [sequence:u64][timestamp_ms:i64][tier:u8][partition:u32]
    ///   [key_present:u8]([key_len:u32][key bytes])?
    ///   [payload_len:u32][payload bytes]
    ///   [header_count:u32]([key_len:u32][key][val_len:u32][val])*
    pub fn serializeStreamRecordsWithPayloads(self: *StreamHandler, records: []const StreamRecord, stream_name: []const u8) ![]u8 {
        const key_size: usize = if (stream_name.len > 0) 1 + 4 + stream_name.len else 1;

        // Expand batch payloads into individual records
        var expanded: [MAX_READ_BATCH]ResolvedRecord = undefined;
        const expanded_count = self.expandRecords(records, &expanded);

        // Compute total buffer size
        var total: usize = 4; // count header
        for (expanded[0..expanded_count]) |e| {
            const hdr_size = responseHeaderSize(e.header_count, e.headers_raw.len);
            total += 8 + 8 + 1 + 4 + key_size + 4 + e.payload.len + 4 + hdr_size;
        }

        const buf = try self.allocator.alloc(u8, total);
        errdefer self.allocator.free(buf);

        var pos: usize = 0;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(expanded_count), .little);
        pos += 4;

        for (expanded[0..expanded_count]) |e| {
            std.mem.writeInt(u64, buf[pos..][0..8], e.sequence, .little);
            pos += 8;
            std.mem.writeInt(i64, buf[pos..][0..8], @as(i64, @intCast(e.timestamp_ms)), .little);
            pos += 8;
            buf[pos] = e.tier;
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], e.partition_index, .little);
            pos += 4;

            // key (stream name for multi-stream / pattern reads)
            if (stream_name.len > 0) {
                buf[pos] = 1;
                pos += 1;
                std.mem.writeInt(u32, buf[pos..][0..4], @intCast(stream_name.len), .little);
                pos += 4;
                @memcpy(buf[pos .. pos + stream_name.len], stream_name);
                pos += stream_name.len;
            } else {
                buf[pos] = 0;
                pos += 1;
            }

            // payload (clean, without batch wrapper)
            std.mem.writeInt(u32, buf[pos..][0..4], @intCast(e.payload.len), .little);
            pos += 4;
            if (e.payload.len > 0) {
                @memcpy(buf[pos .. pos + e.payload.len], e.payload);
                pos += e.payload.len;
            }

            // headers
            std.mem.writeInt(u32, buf[pos..][0..4], e.header_count, .little);
            pos += 4;
            if (e.header_count > 0) {
                pos = writeResponseHeaders(e.headers_raw, e.header_count, buf, pos);
            }
        }

        return buf;
    }

    /// Serialize pattern group read results where each record has its own stream name.
    /// Same wire format as serializeStreamRecordsWithPayloads but with per-record keys.
    fn serializePatternGroupRecords(self: *StreamHandler, records: []const StreamRecord, stream_names_in: []const []const u8) ![]u8 {
        // Expand batch payloads into individual records
        var expanded: [MAX_READ_BATCH]ResolvedRecord = undefined;
        const expanded_count = self.expandRecords(records, &expanded);

        // Build per-record stream name mapping (expand names alongside records)
        var exp_names: [MAX_READ_BATCH][]const u8 = undefined;
        {
            var idx: usize = 0;
            var exp_idx: usize = 0;
            for (records, 0..) |_, ri| {
                if (ri >= stream_names_in.len) break;
                const name = stream_names_in[ri];
                const result_payload = blk: {
                    const result = self.getPayloadAndTier(records[ri].ual_index);
                    var batch_buf: [100]UnpackedRecord = undefined;
                    const n = unpackBatch(result.payload, &batch_buf);
                    break :blk if (n > 0) n else 1;
                };
                var j: usize = 0;
                while (j < result_payload and exp_idx < expanded_count) : ({
                    j += 1;
                    exp_idx += 1;
                }) {
                    exp_names[exp_idx] = name;
                }
                idx += 1;
            }
        }

        // Compute total buffer size
        var total: usize = 4;
        for (expanded[0..expanded_count], 0..) |e, i| {
            const name = exp_names[i];
            const key_size: usize = 1 + 4 + name.len;
            const hdr_size = responseHeaderSize(e.header_count, e.headers_raw.len);
            total += 8 + 8 + 1 + 4 + key_size + 4 + e.payload.len + 4 + hdr_size;
        }

        const buf = try self.allocator.alloc(u8, total);
        errdefer self.allocator.free(buf);

        var pos: usize = 0;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(expanded_count), .little);
        pos += 4;

        for (expanded[0..expanded_count], 0..) |e, i| {
            const name = exp_names[i];

            std.mem.writeInt(u64, buf[pos..][0..8], e.sequence, .little);
            pos += 8;
            std.mem.writeInt(i64, buf[pos..][0..8], @as(i64, @intCast(e.timestamp_ms)), .little);
            pos += 8;
            buf[pos] = e.tier;
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], e.partition_index, .little);
            pos += 4;

            buf[pos] = 1;
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], @intCast(name.len), .little);
            pos += 4;
            @memcpy(buf[pos .. pos + name.len], name);
            pos += name.len;

            std.mem.writeInt(u32, buf[pos..][0..4], @intCast(e.payload.len), .little);
            pos += 4;
            if (e.payload.len > 0) {
                @memcpy(buf[pos .. pos + e.payload.len], e.payload);
                pos += e.payload.len;
            }

            std.mem.writeInt(u32, buf[pos..][0..4], e.header_count, .little);
            pos += 4;
            if (e.header_count > 0) {
                pos = writeResponseHeaders(e.headers_raw, e.header_count, buf, pos);
            }
        }

        return buf;
    }

    /// Append payload to a named stream (used by processing pipelines).
    /// Computes the namespace-qualified name hash for proper stream isolation.
    /// Wraps the payload in batch format for consistency with all other appends.
    pub fn appendPayloadToStream(self: *StreamHandler, stream_name: []const u8, namespace: []const u8, payload: []const u8) !u64 {
        // Wrap raw payload in batch format: [count:u32][payload_len:u32][payload][header_count:u16]
        const batch_size = 4 + 4 + payload.len + 2;
        const batch_buf = try self.allocator.alloc(u8, batch_size);
        defer self.allocator.free(batch_buf);
        std.mem.writeInt(u32, batch_buf[0..4], 1, .little); // count=1
        std.mem.writeInt(u32, batch_buf[4..8], @intCast(payload.len), .little);
        if (payload.len > 0) {
            @memcpy(batch_buf[8 .. 8 + payload.len], payload);
        }
        std.mem.writeInt(u16, batch_buf[8 + payload.len ..][0..2], 0, .little); // header_count=0
        const batch_value = batch_buf[0..batch_size];

        const sptr = self.shard_ptr orelse return error.ShardPtrNotSet;
        const shard = shardFromPtr(sptr);
        const raft_index = try persistence_mod.persistEntry(shard, .stream_append, entry_mod.Flags.NONE, namespace, stream_name, batch_value);
        const stream_id = try self.applyCommittedStreamEntries(shard, raft_index, 0);

        // Register the stream name so it appears in `stream list`
        var ns_reg_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const ns_stream_name = ns_keys.qualifyKey(&ns_reg_buf, namespace, stream_name) catch stream_name;
        self.stream.registerStream(ns_stream_name) catch {};

        return stream_id.sequence;
    }

    /// Read payloads from a named stream (used by processing pipelines).
    /// Uses per-stream StreamID reads for proper stream isolation.
    /// Returns (payloads slice, last StreamID read) so the pipeline can advance its cursor.
    pub fn readPayloadsForStream(self: *StreamHandler, stream_name: []const u8, namespace: []const u8, after_id: StreamID, limit: usize) struct { payloads: []const []const u8, last_id: StreamID } {
        var results: [1000][]const u8 = undefined;
        var count: usize = 0;
        const capped = @min(limit, 1000);

        const ns_hash_u32 = router.namespaceHash(namespace);
        const name_hash = router.nameHash(ns_hash_u32, stream_name);

        var rec_buf: [1000]StreamRecord = undefined;
        const n = self.stream.readStreamAfter(name_hash, after_id, null, rec_buf[0..capped]);

        var last_id = after_id;
        for (rec_buf[0..n]) |rec| {
            if (count >= capped) break;
            const result = self.getPayloadAndTier(rec.ual_index);
            if (result.payload.len > 0) {
                var batch_buf: [100]UnpackedRecord = undefined;
                const batch_n = unpackBatch(result.payload, &batch_buf);
                for (batch_buf[0..batch_n]) |br| {
                    if (count >= capped) break;
                    results[count] = br.payload;
                    count += 1;
                }
            }
            last_id = rec.id;
        }

        const out = self.allocator.alloc([]const u8, count) catch return .{ .payloads = &.{}, .last_id = last_id };
        @memcpy(out, results[0..count]);
        return .{ .payloads = out, .last_id = last_id };
    }

    /// A single record extracted from a batch blob, with payload and raw header bytes.
    const UnpackedRecord = struct {
        payload: []const u8,
        /// Raw header bytes in wire format: [key_len:u16][key][val_len:u16][val]...
        headers_raw: []const u8,
        header_count: u32,
    };

    /// Parse a stored value as batch format and extract individual records
    /// with their payloads AND headers.  Returns 0 if value is not valid batch format.
    fn unpackBatch(value: []const u8, out: []UnpackedRecord) usize {
        if (value.len < 10) return 0;

        const record_count = std.mem.readInt(u32, value[0..4], .little);
        if (record_count == 0 or record_count > 10000) return 0;

        var pos: usize = 4;
        var count: usize = 0;

        var i: u32 = 0;
        while (i < record_count) : (i += 1) {
            if (pos + 4 > value.len) return 0;
            const payload_len = std.mem.readInt(u32, value[pos..][0..4], .little);
            pos += 4;

            if (pos + payload_len > value.len) return 0;
            const payload = value[pos .. pos + payload_len];
            pos += payload_len;

            // Parse header_count and capture raw header bytes
            if (pos + 2 > value.len) return 0;
            const hdr_count = std.mem.readInt(u16, value[pos..][0..2], .little);
            pos += 2;

            const headers_start = pos;
            var h: u16 = 0;
            while (h < hdr_count) : (h += 1) {
                if (pos + 2 > value.len) return 0;
                const hkey_len = std.mem.readInt(u16, value[pos..][0..2], .little);
                pos += 2;
                if (pos + hkey_len > value.len) return 0;
                pos += hkey_len;
                if (pos + 2 > value.len) return 0;
                const hval_len = std.mem.readInt(u16, value[pos..][0..2], .little);
                pos += 2;
                if (pos + hval_len > value.len) return 0;
                pos += hval_len;
            }

            if (count < out.len) {
                out[count] = .{
                    .payload = payload,
                    .headers_raw = value[headers_start..pos],
                    .header_count = hdr_count,
                };
                count += 1;
            }
        }

        if (pos != value.len) return 0;
        return count;
    }

    /// Read a payload from the UAL by entry index (zero-copy).
    /// Returns the message value and the tier byte (0=hot, 1=warm).
    /// Falls back to the warm store when the entry has been evicted from the hot ring.
    fn getPayloadAndTier(self: *StreamHandler, ual_index: u64) struct { payload: []const u8, tier: u8 } {
        // Hot path — entry still in the UAL ring buffer
        if (self.partition.ual.read(ual_index)) |ual_entry| {
            if (ual_entry.commandPayload()) |cmd| {
                return .{ .payload = cmd.value, .tier = 0 };
            }
        }
        // Warm fallback — payload copied to partition warm store on apply()
        if (self.partition.readPayloadWarm(ual_index)) |raw| {
            if (entry_mod.CommandPayload.deserialize(raw)) |cmd| {
                return .{ .payload = cmd.value, .tier = 1 };
            }
        }
        return .{ .payload = "", .tier = 0 };
    }

    /// Free any heap-allocated data in a CommandResult returned by this handler.
    pub fn freeResult(self: *StreamHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .stream_messages => |r| self.allocator.free(r.data),
            .group_messages => |r| self.allocator.free(r.data),
            .group_pending => |r| self.allocator.free(r.data),
            else => {},
        }
    }

    // ── Raft apply ──────────────────────────────────────────────────────

    /// Apply committed Raft entries up to `through_index` for stream types.
    /// Advances `last_applied` so stream indices stay aligned with the Raft log.
    ///
    /// Note: when this loop drains multiple entries (e.g. when commit_index
    /// has advanced faster than per-op apply calls), non-stream entries are
    /// silently skipped. Their projection state is updated separately by the
    /// projection router at `partition.apply` time (during propose) or by
    /// their owning handler's own apply loop. Advancing `last_applied` past
    /// them here is intentional. Trim entries are likewise skipped — they
    /// were already applied in-place by `persistAndApplyTrim`.
    fn applyCommittedStreamEntries(
        self: *StreamHandler,
        shard: *Shard,
        through_index: u64,
        partition_index: u32,
    ) !StreamID {
        const raft = shard.raft_node;
        var last_id = StreamID.MIN;

        while (raft.last_applied < through_index) {
            const next_idx = raft.last_applied + 1;
            if (raft.log.getEntry(next_idx)) |entry| {
                const etype: EntryType = @enumFromInt(entry.header.entry_type);
                if (etype == .stream_append) {
                    last_id = try self.applyStreamAppendEntry(&entry, partition_index);
                }
                // .stream_trim: applied in-place by persistAndApplyTrim; skip.
                // other types: owned by another handler's apply loop; skip.
            }
            raft.last_applied = next_idx;
        }

        shard.syncFlushIfNeeded();
        return last_id;
    }

    fn applyStreamAppendEntry(self: *StreamHandler, entry: *const entry_mod.Entry, partition_index: u32) !StreamID {
        const ual_index = try self.partition.apply(entry);
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return error.InvalidPayload;
        const name_hash = router.nameHash(cmd.namespace_hash, cmd.key);
        return self.stream.appendToStream(name_hash, ual_index, partition_index);
    }

    // ── Replay ──────────────────────────────────────────────────────────

    /// Register stream entry types with the replay registry.
    /// Called during shard init so replaySegments and handleInboxMessage
    /// can rebuild stream state without hardcoded type checks.
    pub fn registerReplay(self: *StreamHandler, registry: *ReplayRegistry) void {
        registry.register(.stream_append, @ptrCast(self), replayEntry);
        registry.register(.stream_trim, @ptrCast(self), replayEntry);
    }

    /// Replay a stream entry — rebuild StreamProjection offset tracking
    /// and stream name registration from a persisted entry.
    ///
    /// `replaySegments` has already called `partition.apply(entry)` before
    /// dispatch, so this callback must NOT re-apply — doing so would
    /// double-write to the partition UAL ring and corrupt `entry_count`.
    /// Use `entry.header.index` directly as the UAL index for stream lookups.
    fn replayEntry(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
        const self: *StreamHandler = @ptrCast(@alignCast(ctx));
        const etype: EntryType = @enumFromInt(entry.header.entry_type);

        if (etype == .stream_append) {
            if (entry_mod.CommandPayload.deserialize(entry.payload)) |cmd| {
                const name_hash = router.nameHash(cmd.namespace_hash, cmd.key);
                _ = self.stream.appendToStream(name_hash, entry.header.index, 0) catch {};
                self.stream.registerStream(cmd.key) catch {};
            }
        }

        if (etype == .stream_trim) {
            if (entry_mod.CommandPayload.deserialize(entry.payload)) |cmd| {
                if (cmd.key.len >= 16) {
                    const ts = std.mem.readInt(u64, cmd.key[0..8], .little);
                    const seq = std.mem.readInt(u64, cmd.key[8..16], .little);
                    const nh = if (cmd.value.len >= 8) std.mem.readInt(u64, cmd.value[0..8], .little) else 0;
                    _ = self.stream.trimStream(nh, .{ .timestamp_ms = ts, .sequence = seq });
                }
            }
        }
    }

    // ── Trim Persistence ────────────────────────────────────────────────

    /// Encode a trim as a stream_trim entry and persist through Raft.
    /// Format: key = [timestamp_ms:u64 LE][sequence:u64 LE], value = [name_hash:u64 LE]
    /// After Raft commit, applies the trim locally to the projection.
    /// Returns the number of records trimmed locally.
    fn persistAndApplyTrim(self: *StreamHandler, namespace: []const u8, name_hash: u64, trim_id: StreamID) u64 {
        // Encode trim target as the entry key (16 bytes) and name_hash as value (8 bytes)
        var key_buf: [16]u8 = undefined;
        std.mem.writeInt(u64, key_buf[0..8], trim_id.timestamp_ms, .little);
        std.mem.writeInt(u64, key_buf[8..16], trim_id.sequence, .little);

        var val_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, val_buf[0..8], name_hash, .little);

        // Persist through Raft — on commit, replicas will see this via replayEntry
        if (self.shard_ptr) |sptr| {
            const shard = shardFromPtr(sptr);
            _ = persistence_mod.persistEntry(shard, .stream_trim, entry_mod.Flags.NONE, namespace, &key_buf, &val_buf) catch {};
        }

        // Apply locally: trim the projection
        return self.stream.trimStream(name_hash, trim_id);
    }

    /// Public interface for background tasks (retention enforcer) to persist trims.
    pub fn persistTrim(self: *StreamHandler, name_hash: u64, trim_id: StreamID) u64 {
        return self.persistAndApplyTrim("", name_hash, trim_id);
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    /// Cast opaque shard pointer to a Shard-like type for persistEntry().
    /// Uses anytype to avoid circular import.
    fn shardFromPtr(ptr: *anyopaque) *Shard {
        return @ptrCast(@alignCast(ptr));
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Wire Decode Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Result from group name decoding — tracks whether wire format was used
/// so callers can decide whether to namespace-qualify.
const GroupDecode = struct {
    name: []const u8,
    wire: bool,
};

/// Result from group+consumer pair decoding.
const GroupConsumerDecode = struct {
    group: []const u8,
    consumer: []const u8,
    wire: bool,
};

/// Decode a group name from wire format: [group_len:u16][group]
/// Falls back to treating the entire value as the group name (unit test compat).
fn decodeGroupName(value: []const u8) ?GroupDecode {
    if (value.len >= 2) {
        var reader = WireReader.init(value);
        if (reader.readLengthPrefixed(u16)) |name| {
            if (name.len > 0) return .{ .name = name, .wire = true };
        }
    }
    // Fallback: raw value
    if (value.len > 0) return .{ .name = value, .wire = false };
    return null;
}

/// Decode group + consumer pair from wire format.
/// Wire: [group_len:u16][group][consumer_len:u16][consumer]
/// Falls back to raw value = group name, namespace = consumer (unit test compat).
fn decodeGroupConsumer(req: Request) ?GroupConsumerDecode {
    if (req.value.len >= 2) {
        var reader = WireReader.init(req.value);
        if (reader.readPair(u16, u16)) |pair| {
            if (pair.key.len > 0) return .{ .group = pair.key, .consumer = pair.value, .wire = true };
        }
        // Try single length-prefixed (for group-only wire format used as pair)
        var reader2 = WireReader.init(req.value);
        if (reader2.readLengthPrefixed(u16)) |name| {
            if (name.len > 0) return .{ .group = name, .consumer = "", .wire = true };
        }
    }
    // Fallback: raw value = group, namespace = consumer
    if (req.value.len > 0) return .{ .group = req.value, .consumer = if (req.namespace.len > 0) req.namespace else "", .wire = false };
    return null;
}

/// Namespace-qualify a group name when decoded from wire format.
/// Wire-format requests carry the real namespace; fallback (unit test) requests
/// abuse the namespace field, so we only qualify for wire-format decodes.
fn resolveGroupName(buf: *[ns_keys.MAX_QUALIFIED_KEY]u8, namespace: []const u8, raw_name: []const u8, is_wire: bool) []const u8 {
    if (is_wire) {
        return ns_keys.qualifyKey(buf, namespace, raw_name) catch raw_name;
    }
    return raw_name;
}

/// Serialize a list of names in the standard walk wire format.
/// Wire format: [count:u32]([name_len:u16][name])*[has_more:u8][cursor_len:u16][cursor]
fn serializeNameList(allocator: Allocator, names: []const []const u8, stream: *const StreamProjection) ![]u8 {
    // Stream list wire format (matches CLI expectations):
    // [count:u32] ([name_len:u32][name][partition_count:u32])* [has_more:u8] [cursor_len:u16]
    var total: usize = 4; // count
    for (names) |name| {
        total += 4 + name.len + 4; // name_len:u32 + name + partition_count:u32
    }
    total += 1 + 2; // has_more:u8 + cursor_len:u16

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var pos: usize = 0;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(names.len), .little);
    pos += 4;

    for (names) |name| {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(name.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..name.len], name);
        pos += name.len;
        // partition_count from stream metadata
        const pc = stream.getPartitionCount(name);
        std.mem.writeInt(u32, buf[pos..][0..4], pc, .little);
        pos += 4;
    }

    // has_more = 0, cursor_len = 0
    buf[pos] = 0;
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], 0, .little);
    pos += 2;

    return buf;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Response Serialization — CommandResult → Wire Response
// ═══════════════════════════════════════════════════════════════════════════════

/// Convert a stream CommandResult to a wire response and queue it on the connection.
fn sendStreamResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .ok => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .err => |e| {
            const status = errorCodeToStatus(e.code);
            shard.sendErrorResponse(conn, request_id, status, e.message);
        },
        .kv_not_found => {
            shard.sendErrorResponse(conn, request_id, .not_found, "not found");
        },
        .stream_append_ok => |a| {
            // Send sequence + timestamp_ms as [u64 LE][i64 LE] = 16 bytes
            var buf: [16]u8 = undefined;
            std.mem.writeInt(u64, buf[0..8], a.sequence, .little);
            std.mem.writeInt(i64, buf[8..16], a.timestamp_ms, .little);
            shard.sendOkResponse(conn, request_id, &buf);
        },
        .stream_messages => |m| {
            shard.sendOkResponse(conn, request_id, m.data);
        },
        .stream_info => |i| {
            // Serialize: [first_ts:u64][first_seq:u64][last_ts:u64][last_seq:u64][count:u64][bytes:u64][partition_count:u32][retention_age_s:u64][retention_count:u64][retention_bytes:u64]
            var buf: [76]u8 = undefined;
            var off: usize = 0;
            std.mem.writeInt(u64, buf[off..][0..8], i.first_timestamp_ms, .little);
            off += 8;
            std.mem.writeInt(u64, buf[off..][0..8], i.first_seq, .little);
            off += 8;
            std.mem.writeInt(u64, buf[off..][0..8], i.last_timestamp_ms, .little);
            off += 8;
            std.mem.writeInt(u64, buf[off..][0..8], i.last_seq, .little);
            off += 8;
            std.mem.writeInt(u64, buf[off..][0..8], i.count, .little);
            off += 8;
            std.mem.writeInt(u64, buf[off..][0..8], i.bytes, .little);
            off += 8;
            std.mem.writeInt(u32, buf[off..][0..4], i.partition_count, .little);
            off += 4;
            std.mem.writeInt(u64, buf[off..][0..8], i.retention_age_s, .little);
            off += 8;
            std.mem.writeInt(u64, buf[off..][0..8], i.retention_count, .little);
            off += 8;
            std.mem.writeInt(u64, buf[off..][0..8], i.retention_bytes, .little);
            shard.sendOkResponse(conn, request_id, &buf);
        },
        .stream_trimmed => |t| {
            // Send deleted_count + first_seq as two u64 LE values (16 bytes)
            var buf: [16]u8 = undefined;
            std.mem.writeInt(u64, buf[0..8], t.deleted_count, .little);
            std.mem.writeInt(u64, buf[8..16], t.first_seq, .little);
            shard.sendOkResponse(conn, request_id, &buf);
        },
        .group_joined => |j| {
            // Serialize: [generation_id:u64][partition_count:u32][partition:u32]*
            const part_count: u32 = @intCast(j.assigned_partitions.len);
            var buf: [256]u8 = undefined;
            std.mem.writeInt(u64, buf[0..8], j.generation_id, .little);
            std.mem.writeInt(u32, buf[8..12], part_count, .little);
            var off: usize = 12;
            for (j.assigned_partitions) |p| {
                if (off + 4 > buf.len) break;
                std.mem.writeInt(u32, buf[off..][0..4], p, .little);
                off += 4;
            }
            shard.sendOkResponse(conn, request_id, buf[0..off]);
        },
        .group_messages => |m| {
            shard.sendOkResponse(conn, request_id, m.data);
        },
        .group_pending => |p| {
            shard.sendOkResponse(conn, request_id, p.data);
        },
        else => {
            shard.sendErrorResponse(conn, request_id, .internal_error, "unhandled stream response");
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
        .stream_not_found => .not_found,
        .stream_offset_out_of_range => .bad_request,
        .stream_partition_not_found => .not_found,
        .group_not_found => .not_found,
        .group_rebalancing => .internal_error,
        .group_consumer_not_found => .not_found,
        .conflict => .conflict,
        else => .internal_error,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization
// ═══════════════════════════════════════════════════════════════════════════════

/// Serialize consumer group info.
/// Wire format: [pel_count:u64][member_count:u32][created_at_ns:u64]
fn serializeGroupInfo(allocator: Allocator, group: *stream_mod.ConsumerGroup) ![]u8 {
    const total: usize = 8 + 4 + 8; // pel_count + member_count + created_at_ns
    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var offset: usize = 0;

    std.mem.writeInt(u64, buf[offset..][0..8], @intCast(group.pelCount()), .little);
    offset += 8;

    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(group.memberCount()), .little);
    offset += 4;

    std.mem.writeInt(u64, buf[offset..][0..8], group.created_at_ns, .little);

    return buf;
}

/// Serialize PEL entries from groupPending().
/// Wire format: [count:u32]([timestamp_ms:u64][sequence:u64][delivery_count:u32][consumer_len:u16][consumer])*
fn serializePendingEntries(allocator: Allocator, entries: []const PendingEntry) ![]u8 {
    var total: usize = 4; // count header
    for (entries) |e| {
        total += 8 + 8 + 4 + 2 + e.consumer.len; // ts + seq + delivery_count + consumer_len + consumer
    }

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var pos: usize = 0;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(entries.len), .little);
    pos += 4;

    for (entries) |e| {
        std.mem.writeInt(u64, buf[pos..][0..8], e.id.timestamp_ms, .little);
        pos += 8;
        std.mem.writeInt(u64, buf[pos..][0..8], e.id.sequence, .little);
        pos += 8;
        std.mem.writeInt(u32, buf[pos..][0..4], e.delivery_count, .little);
        pos += 4;
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(e.consumer.len), .little);
        pos += 2;
        if (e.consumer.len > 0) {
            @memcpy(buf[pos .. pos + e.consumer.len], e.consumer);
            pos += e.consumer.len;
        }
    }

    return buf;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

/// Helper to build a test request.
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

/// Build a batch-format value for a single payload with no headers.
/// Batch format: [count:u32][payload_len:u32][payload][header_count:u16]
fn makeBatchValue(buf: []u8, payload: []const u8) []const u8 {
    const total = 4 + 4 + payload.len + 2;
    std.mem.writeInt(u32, buf[0..4], 1, .little);
    std.mem.writeInt(u32, buf[4..8], @intCast(payload.len), .little);
    if (payload.len > 0) {
        @memcpy(buf[8 .. 8 + payload.len], payload);
    }
    std.mem.writeInt(u16, buf[8 + payload.len ..][0..2], 0, .little);
    return buf[0..total];
}

/// Build a batch-format value for a single payload with one header.
/// Batch format: [count:u32][payload_len:u32][payload][header_count:u16][key_len:u16][key][val_len:u16][val]
fn makeBatchValueWithHeader(buf: []u8, payload: []const u8, hdr_key: []const u8, hdr_val: []const u8) []const u8 {
    const total = 4 + 4 + payload.len + 2 + 2 + hdr_key.len + 2 + hdr_val.len;
    var pos: usize = 0;
    std.mem.writeInt(u32, buf[pos..][0..4], 1, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(payload.len), .little);
    pos += 4;
    if (payload.len > 0) {
        @memcpy(buf[pos .. pos + payload.len], payload);
        pos += payload.len;
    }
    std.mem.writeInt(u16, buf[pos..][0..2], 1, .little); // header_count=1
    pos += 2;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(hdr_key.len), .little);
    pos += 2;
    @memcpy(buf[pos .. pos + hdr_key.len], hdr_key);
    pos += hdr_key.len;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(hdr_val.len), .little);
    pos += 2;
    @memcpy(buf[pos .. pos + hdr_val.len], hdr_val);
    return buf[0..total];
}

test "stream handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    StreamHandler.register(&dispatcher);

    // Stream opcodes
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_append)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_read)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_trim)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_info)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_list)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_create)] != null);

    // Consumer group opcodes
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_create)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_join)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_leave)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_read)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_ack)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_nack)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_delete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_info)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_pending)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_touch)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_group_claim)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.stream_alter)] != null);

    try testing.expectEqual(@as(u16, 17), dispatcher.handler_count);
}

test "stream handler: append" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var vbuf1: [64]u8 = undefined;
    const val1 = makeBatchValue(&vbuf1, "payload1");
    const result = handler.handleCommand(makeRequest(.stream_append, "events", val1, ""));
    var first_ts: i64 = 0;
    switch (result) {
        .stream_append_ok => |a| {
            // StreamID sequence is 0-based within a millisecond
            try testing.expect(a.timestamp_ms > 0);
            first_ts = a.timestamp_ms;
        },
        else => return error.TestUnexpectedResult,
    }

    // Second append
    var vbuf2: [64]u8 = undefined;
    const val2 = makeBatchValue(&vbuf2, "payload2");
    const r2 = handler.handleCommand(makeRequest(.stream_append, "events", val2, ""));
    switch (r2) {
        .stream_append_ok => |a| {
            // Must have a valid timestamp; sequence may differ based on timing
            try testing.expect(a.timestamp_ms >= first_ts);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: append empty stream name" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();
    var vbuf_data: [64]u8 = undefined;
    const val_data = makeBatchValue(&vbuf_data, "data");
    const result = handler.handleCommand(makeRequest(.stream_append, "", val_data, ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: read" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // Append 3 messages
    var vbuf_a: [64]u8 = undefined;
    var vbuf_b: [64]u8 = undefined;
    var vbuf_c: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vbuf_a, "a"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vbuf_b, "b"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vbuf_c, "c"), ""));

    // Read all
    const result = handler.handleCommand(makeRequest(.stream_read, "s1", "", ""));
    switch (result) {
        .stream_messages => |m| {
            defer handler.freeResult(result);
            // Parse count from serialized data
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 3), count);
            // next_sequence is the last record's StreamID sequence (0-based)
            try testing.expect(m.next_sequence >= 0);
            try testing.expect(m.next_timestamp_ms > 0);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: read with limit" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // Append 5 messages
    var vb1: [64]u8 = undefined;
    var vb2: [64]u8 = undefined;
    var vb3: [64]u8 = undefined;
    var vb4: [64]u8 = undefined;
    var vb5: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vb1, "1"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vb2, "2"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vb3, "3"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vb4, "4"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vb5, "5"), ""));

    // Read with limit 2
    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addU32(.limit, 2);
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.stream_read, "s1", "", opts));
    switch (result) {
        .stream_messages => |m| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 2), count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: trim" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // Append 5 messages
    var vba: [64]u8 = undefined;
    var vbb: [64]u8 = undefined;
    var vbc: [64]u8 = undefined;
    var vbd: [64]u8 = undefined;
    var vbe: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vba, "a"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vbb, "b"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vbc, "c"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vbd, "d"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vbe, "e"), ""));

    // Trim first 3 records (bare value = count-based trim)
    const result = handler.handleCommand(makeRequest(.stream_trim, "s1", "3", ""));
    switch (result) {
        .stream_trimmed => |t| {
            try testing.expectEqual(@as(u64, 3), t.deleted_count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: info" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // Append some messages
    var vinfo_a: [64]u8 = undefined;
    var vinfo_b: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vinfo_a, "a"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vinfo_b, "b"), ""));

    const result = handler.handleCommand(makeRequest(.stream_info, "s1", "", ""));
    switch (result) {
        .stream_info => |info| {
            // Two records with sequentially increasing StreamIDs
            try testing.expectEqual(@as(u64, 2), info.count);
            try testing.expectEqual(@as(u32, 1), info.partition_count);
            try testing.expect(info.last_timestamp_ms > 0);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: create is ok (implicit)" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();
    const result = handler.handleCommand(makeRequest(.stream_create, "new_stream", "", ""));
    switch (result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: group lifecycle" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // Create group (stream=key, group_name=value)
    const create_result = handler.handleCommand(makeRequest(.stream_group_create, "s1", "my-group", ""));
    switch (create_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }

    // Create duplicate → error
    const dup_result = handler.handleCommand(makeRequest(.stream_group_create, "s1", "my-group", ""));
    switch (dup_result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.already_exists, e.code),
        else => return error.TestUnexpectedResult,
    }

    // Join group (namespace=member_id)
    var join_req = makeRequest(.stream_group_join, "s1", "my-group", "");
    join_req.namespace = "member-1";
    const join_result = handler.handleCommand(join_req);
    switch (join_result) {
        .group_joined => {},
        else => return error.TestUnexpectedResult,
    }

    // Leave group
    var leave_req = makeRequest(.stream_group_leave, "s1", "my-group", "");
    leave_req.namespace = "member-1";
    const leave_result = handler.handleCommand(leave_req);
    switch (leave_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }

    // Delete group
    const del_result = handler.handleCommand(makeRequest(.stream_group_delete, "s1", "my-group", ""));
    switch (del_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }

    // Delete again → not found
    const del2_result = handler.handleCommand(makeRequest(.stream_group_delete, "s1", "my-group", ""));
    switch (del2_result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.group_not_found, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: group read and ack" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // Append messages
    var vm1: [64]u8 = undefined;
    var vm2: [64]u8 = undefined;
    var vm3: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vm1, "msg1"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vm2, "msg2"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vm3, "msg3"), ""));

    // Create group
    _ = handler.handleCommand(makeRequest(.stream_group_create, "s1", "cg1", ""));

    // First read — PEL delivers all 3 records, adds to PEL
    var first_id_ts: u64 = 0;
    var first_id_seq: u64 = 0;
    const read_result = handler.handleCommand(makeRequest(.stream_group_read, "s1", "cg1", ""));
    switch (read_result) {
        .group_messages => |m| {
            defer handler.freeResult(read_result);
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 3), count);
            // Extract first record's StreamID: [count:u32][sequence:u64][timestamp_ms:i64]...
            first_id_seq = std.mem.readInt(u64, m.data[4..12], .little);
            first_id_ts = std.mem.readInt(u64, m.data[12..20], .little);
        },
        else => return error.TestUnexpectedResult,
    }

    // Second read with no new appends — no NEW records after last_delivered_id
    const read2_result = handler.handleCommand(makeRequest(.stream_group_read, "s1", "cg1", ""));
    switch (read2_result) {
        .group_messages => |m| {
            defer handler.freeResult(read2_result);
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), count); // no new records
        },
        else => return error.TestUnexpectedResult,
    }

    // Append a new message
    var vm4: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vm4, "msg4"), ""));

    // Third read — delivers only the new msg4
    const read3_result = handler.handleCommand(makeRequest(.stream_group_read, "s1", "cg1", ""));
    switch (read3_result) {
        .group_messages => |m| {
            defer handler.freeResult(read3_result);
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 1), count); // only msg4
        },
        else => return error.TestUnexpectedResult,
    }

    // Ack via proper wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][ts:u64][seq:u64]
    var ack_wire: [2 + 3 + 2 + 7 + 4 + 16]u8 = undefined;
    std.mem.writeInt(u16, ack_wire[0..2], 3, .little); // "cg1"
    @memcpy(ack_wire[2..5], "cg1");
    std.mem.writeInt(u16, ack_wire[5..7], 7, .little); // "default"
    @memcpy(ack_wire[7..14], "default");
    std.mem.writeInt(u32, ack_wire[14..18], 1, .little); // count=1
    std.mem.writeInt(u64, ack_wire[18..26], first_id_ts, .little);
    std.mem.writeInt(u64, ack_wire[26..34], first_id_seq, .little);
    const ack_req = makeRequest(.stream_group_ack, "s1", &ack_wire, "");
    const ack_result = handler.handleCommand(ack_req);
    switch (ack_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: group info" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // Create group and join
    _ = handler.handleCommand(makeRequest(.stream_group_create, "s1", "cg1", ""));
    var join_req = makeRequest(.stream_group_join, "s1", "cg1", "");
    join_req.namespace = "m1";
    _ = handler.handleCommand(join_req);

    const result = handler.handleCommand(makeRequest(.stream_group_info, "s1", "cg1", ""));
    switch (result) {
        .group_pending => |p| {
            defer handler.freeResult(result);
            // Parse member_count from serialized data (at offset 8)
            const member_count = std.mem.readInt(u32, p.data[8..12], .little);
            try testing.expectEqual(@as(u32, 1), member_count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: group not found errors" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // Join non-existent group — auto-creates the group
    const join_result = handler.handleCommand(makeRequest(.stream_group_join, "s1", "nope", ""));
    switch (join_result) {
        .group_joined => {},
        else => return error.TestUnexpectedResult,
    }

    // Ack non-existent group — wire format with fake StreamID
    var ack_wire: [2 + 14 + 2 + 1 + 4 + 16]u8 = undefined;
    std.mem.writeInt(u16, ack_wire[0..2], 14, .little);
    @memcpy(ack_wire[2..16], "no_such_group\x00");
    std.mem.writeInt(u16, ack_wire[16..18], 1, .little);
    @memcpy(ack_wire[18..19], "x");
    std.mem.writeInt(u32, ack_wire[19..23], 1, .little); // count=1
    std.mem.writeInt(u64, ack_wire[23..31], 1000, .little); // ts
    std.mem.writeInt(u64, ack_wire[31..39], 0, .little); // seq
    const ack_req = makeRequest(.stream_group_ack, "s1", &ack_wire, "");
    const ack_result = handler.handleCommand(ack_req);
    switch (ack_result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.group_not_found, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: pre-route by stream" {
    const req1 = makeRequest(.stream_append, "stream-a", "", "");
    const req2 = makeRequest(.stream_append, "stream-a", "", "");
    const req3 = makeRequest(.stream_append, "stream-b", "", "");

    // Same stream → same hash
    try testing.expectEqual(StreamHandler.preRouteByStream(req1), StreamHandler.preRouteByStream(req2));

    // Different stream → different hash
    try testing.expect(StreamHandler.preRouteByStream(req1) != StreamHandler.preRouteByStream(req3));

    // Empty → 0
    const req_empty = makeRequest(.stream_append, "", "", "");
    try testing.expectEqual(@as(?u64, 0), StreamHandler.preRouteByStream(req_empty));

    // Same stream, different namespace → different hash (namespace isolation)
    var req_ns = makeRequest(.stream_append, "stream-a", "", "");
    req_ns.namespace = "other";
    try testing.expect(StreamHandler.preRouteByStream(req1) != StreamHandler.preRouteByStream(req_ns));
}
