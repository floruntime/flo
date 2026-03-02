//! Stream Handler — registers stream and consumer-group opcodes with Dispatcher.
//!
//! Read operations (read, info, group_pending) query the StreamProjection for
//! offset → ual_index mappings, then read payloads directly from the UAL
//! (zero-copy when the entry is contiguous in the hot ring).
//!
//! Write operations (append) build a CommandEntry, persist it to the UAL via
//! Partition.apply(), then update the StreamProjection's offset tracking.
//! Raft propose will be added when the full consensus pipeline is connected.
//!
//! ## Design (UNIFIED_STORAGE_DESIGN.md — P6: Zero-Copy Stream Path)
//!
//! Stream data entries live in the UAL and are read directly — no materialization
//! step. The UAL entry IS the stream record. The StreamProjection only maintains
//! lightweight offset → (ual_index, timestamp_ns) mappings.
//!
//! ## Opcode Ranges
//!
//!   Streams:        0x10–0x1F (append, read, trim, info, subscribe, list, create, alter)
//!   Consumer Groups: 0x20–0x2C (create, join, leave, read, ack, nack, claim, pending, touch, info, delete)
//!
//! ## Data Model
//!
//! The StreamProjection tracks offset → (ual_index, timestamp_ns) mappings.
//! Actual message payloads live in the UAL at the recorded ual_index.
//! Consumer groups track per-member committed offsets.

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
const OffsetEntry = stream_mod.OffsetEntry;
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

// ═══════════════════════════════════════════════════════════════════════════════
// StreamHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const StreamHandler = struct {
    stream: *StreamProjection,
    partition: *Partition,
    allocator: Allocator,

    /// Maximum number of messages in a single read response.
    const MAX_READ_BATCH: usize = 1000;
    const DEFAULT_READ_BATCH: usize = 100;

    pub fn init(allocator: Allocator, partition: *Partition) StreamHandler {
        return .{
            .stream = &partition.stream,
            .partition = partition,
            .allocator = allocator,
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

        // Consumer group operations (0x20–0x2C)
        dispatcher.registerWithRoute(.stream_group_create, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_join, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_leave, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_read, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_ack, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_nack, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_delete, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_info, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_pending, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_touch, dispatchStream, preRouteByStream);
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
                .ok => shard.namespace_handler.markNamespaceHasData(req.namespace),
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
                shard.namespace_handler.markNamespaceHasData(req.namespace);
                if (req.key.len > 0) {
                    shard.waiter_pool.notify(.stream_read, req.key, @import("../node/shard.zig").resolveStreamWaiter, @ptrCast(shard));
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

            // Register waiter with stream's current high water mark
            const hwm = shard.defaultPartition().stream.highWaterMark();
            _ = shard.waiter_pool.register(.{
                .kind = .stream_read,
                .fd = conn.fd,
                .request_id = req.header.request_id,
                .key = req.key,
                .min_version = hwm,
                .timeout_ms = bms,
            });
            conn.response_deferred = true;
            return;
        }

        // Non-blocking read — standard path
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

        const timestamp_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
        const next_index = self.partition.ual.max_index + 1;
        const payload_value = if (req.value.len > 0) req.value else "";

        // Build command entry: key = stream name, value = message payload
        const payload_size = entry_mod.COMMAND_PREFIX_SIZE + req.key.len + payload_value.len;
        const payload_buf = self.allocator.alloc(u8, payload_size) catch {
            return .{ .err = .{ .code = .internal_error, .message = "alloc failed" } };
        };
        defer self.allocator.free(payload_buf);

        const entry = entry_mod.buildCommandEntry(
            .stream_append,
            entry_mod.Flags.NONE,
            self.partition.current_term,
            next_index,
            timestamp_ns,
            router.namespaceHash(req.namespace),
            req.key,
            payload_value,
            payload_buf,
        ) orelse {
            return .{ .err = .{ .code = .internal_error, .message = "entry build failed" } };
        };

        // Persist to UAL (ring buffer) and route through projections
        const ual_index = self.partition.apply(&entry) catch {
            return .{ .err = .{ .code = .internal_error, .message = "UAL append failed" } };
        };

        // Track offset → ual_index in stream projection
        const ns_hash = router.namespaceHash(req.namespace);
        const name_hash = router.nameHash(ns_hash, req.key);
        const offset = self.stream.append(ual_index, timestamp_ns, name_hash, partition_index) catch {
            return .{ .err = .{ .code = .internal_error, .message = "append failed" } };
        };

        // Register stream name for listing (namespace-qualified)
        var ns_reg_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const ns_stream_name = ns_keys.qualifyKey(&ns_reg_buf, req.namespace, req.key) catch req.key;
        self.stream.registerStream(ns_stream_name) catch {};

        const timestamp_ms = @as(i64, @intCast(timestamp_ns / 1_000_000));
        return .{ .stream_append_ok = .{
            .sequence = offset,
            .timestamp_ms = timestamp_ms,
        } };
    }

    // ── READ ────────────────────────────────────────────────────────────

    fn handleRead(self: *StreamHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "stream name is required" } };
        }

        const limit = req.getLimit() orelse DEFAULT_READ_BATCH;
        const capped = @min(limit, MAX_READ_BATCH);

        // Determine start offset
        var start_offset: u64 = 1;

        // Check for tail flag — start AFTER the current HWM so only new data is returned
        if (req.findOption(.stream_tail) != null) {
            const hwm = self.stream.highWaterMark();
            start_offset = hwm + 1;
        } else if (req.findOption(.stream_start)) |opt| {
            if (opt.asStreamId()) |sid| {
                // Use the sequence component as the offset
                start_offset = sid.sequence;
            }
        }

        // Determine end offset
        var end_offset = start_offset + capped;
        if (req.findOption(.stream_end)) |opt| {
            if (opt.asStreamId()) |sid| {
                end_offset = sid.sequence + 1;
            }
        }

        // Read the range — filter by namespace-qualified stream name hash for isolation
        const ns_hash = router.namespaceHash(req.namespace);
        const name_hash = router.nameHash(ns_hash, req.key);
        var buf: [MAX_READ_BATCH]OffsetEntry = undefined;
        const out = buf[0..capped];

        // Check for partition filter
        var count: usize = 0;
        if (req.findOption(.partition)) |opt| {
            if (opt.asU32()) |p| {
                count = self.stream.readRangeForStreamPartition(start_offset, end_offset, name_hash, p, out);
            } else {
                count = self.stream.readRangeForStream(start_offset, end_offset, name_hash, out);
            }
        } else if (req.findOption(.partition_key)) |opt| {
            const pk_bytes = opt.data;
            if (pk_bytes.len > 0) {
                const pc = self.stream.getPartitionCount(req.key);
                const pi: u32 = @intCast(std.hash.Wyhash.hash(0, pk_bytes) % pc);
                count = self.stream.readRangeForStreamPartition(start_offset, end_offset, name_hash, pi, out);
            } else {
                count = self.stream.readRangeForStream(start_offset, end_offset, name_hash, out);
            }
        } else {
            count = self.stream.readRangeForStream(start_offset, end_offset, name_hash, out);
        }

        // readRange clamps start to max(start_offset, trim_offset+1), so compute actual
        const actual_start = @max(start_offset, self.stream.trim_offset + 1);

        // Serialize with full wire format including payloads
        const data = self.serializeMessagesWithPayloads(out[0..count], actual_start) catch {
            return .{ .err = .{ .code = .internal_error, .message = "read serialization failed" } };
        };

        const next_ts: u64 = if (count > 0) out[count - 1].timestamp_ns / 1_000_000 else 0;
        const next_seq: u64 = actual_start + count;

        return .{ .stream_messages = .{
            .data = data,
            .next_timestamp_ms = next_ts,
            .next_sequence = next_seq,
        } };
    }

    // ── TRIM ────────────────────────────────────────────────────────────

    fn handleTrim(self: *StreamHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "stream name is required" } };
        }

        // Get trim target from value (as offset number) or options
        var up_to: u64 = 0;

        if (req.findOption(.stream_end)) |opt| {
            if (opt.asStreamId()) |sid| {
                up_to = sid.sequence;
            }
        }

        if (up_to == 0 and req.value.len > 0) {
            // Try parsing value as a decimal offset
            up_to = std.fmt.parseInt(u64, req.value, 10) catch 0;
        }

        if (up_to == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "trim offset is required" } };
        }

        const deleted = self.stream.trim(up_to);

        return .{ .stream_trimmed = .{
            .deleted_count = deleted,
            .first_seq = up_to + 1,
        } };
    }

    // ── INFO ────────────────────────────────────────────────────────────

    fn handleInfo(self: *StreamHandler, req: Request) CommandResult {
        const hwm = self.stream.highWaterMark();
        const count = self.stream.trackedOffsets();
        const pc = self.stream.getPartitionCount(req.key);

        return .{ .stream_info = .{
            .first_timestamp_ms = 0,
            .first_seq = if (count > 0) @as(u64, 1) else 0,
            .last_timestamp_ms = 0,
            .last_seq = hwm,
            .count = count,
            .bytes = 0, // Not tracked by projection
            .partition_count = pc,
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
            self.stream.registerStreamMetadata(req.key, partition_count) catch {};
        }
        return .ok;
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

        const now_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

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
        const now_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

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
        // Wire format: [group_len:u16][group][consumer_len:u16][consumer]
        const pair = decodeGroupConsumer(req) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, pair.group, pair.wire);
        const consumer_id = if (pair.consumer.len > 0) pair.consumer else "default";

        // Auto-create group and join consumer if not exists
        const now_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
        self.stream.createGroup(group_name, now_ns) catch |err| {
            if (err != error.AlreadyExists) {
                return .{ .err = .{ .code = .internal_error, .message = "group creation failed" } };
            }
        };
        _ = self.stream.joinGroup(group_name, consumer_id, now_ns) catch {};

        const group = self.stream.getGroup(group_name) orelse {
            return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
        };

        const limit = req.getLimit() orelse DEFAULT_READ_BATCH;
        const capped = @min(limit, MAX_READ_BATCH);

        // Read from group's committed offset, filtered by namespace-qualified stream name
        const start = group.committed_offset + 1;
        const end = start + capped;

        const ns_hash = router.namespaceHash(req.namespace);
        const name_hash = router.nameHash(ns_hash, req.key);
        var buf: [MAX_READ_BATCH]OffsetEntry = undefined;
        const count = self.stream.readRangeForStream(start, end, name_hash, buf[0..capped]);

        const actual_start = @max(start, self.stream.trim_offset + 1);
        const data = self.serializeMessagesWithPayloads(buf[0..count], actual_start) catch {
            return .{ .err = .{ .code = .internal_error, .message = "group read serialization failed" } };
        };

        return .{ .group_messages = .{ .data = data } };
    }

    // ── GROUP ACK ───────────────────────────────────────────────────────

    fn handleGroupAck(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][seq:u64]*
        // Fallback: raw value as group name (unit tests)
        var reader = WireReader.init(req.value);
        const raw_group = reader.readLengthPrefixed(u16) orelse {
            // Fallback: treat raw value as group name (unit tests — no namespace qualification)
            return self.handleGroupAckFallback(req);
        };
        // Try reading consumer (may not exist for simple format)
        _ = reader.readLengthPrefixed(u16); // consumer — consume but don't need

        // Namespace-qualify for isolation (wire format)
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, raw_group, true);

        // Read sequence array
        var offset: u64 = 0;
        if (reader.readU32()) |count| {
            // Read sequences and find the max as the commit offset
            for (0..count) |_| {
                if (reader.readU64()) |seq| {
                    offset = @max(offset, seq);
                }
            }
        }

        if (offset == 0) {
            // Try fallback from options
            if (req.findOption(.stream_start)) |opt| {
                if (opt.asStreamId()) |sid| {
                    offset = sid.sequence;
                }
            }
        }

        if (offset == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "ack offset is required" } };
        }

        if (group_name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        }

        self.stream.commitOffset(group_name, offset) catch |err| {
            return switch (err) {
                error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
            };
        };

        return .ok;
    }

    /// Fallback for unit tests that pass raw value as group name
    fn handleGroupAckFallback(self: *StreamHandler, req: Request) CommandResult {
        const group_name = if (req.value.len > 0) req.value else {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

        var offset: u64 = 0;
        if (req.findOption(.stream_start)) |opt| {
            if (opt.asStreamId()) |sid| {
                offset = sid.sequence;
            }
        }
        if (offset == 0 and req.namespace.len > 0) {
            offset = std.fmt.parseInt(u64, req.namespace, 10) catch 0;
        }
        if (offset == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "ack offset is required" } };
        }

        self.stream.commitOffset(group_name, offset) catch |err| {
            return switch (err) {
                error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
            };
        };

        return .ok;
    }

    // ── GROUP NACK ──────────────────────────────────────────────────────

    fn handleGroupNack(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][seq:u64]*
        var reader = WireReader.init(req.value);
        const raw_group = reader.readLengthPrefixed(u16) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };
        _ = reader.readLengthPrefixed(u16); // consumer

        // Namespace-qualify for isolation (always wire format here)
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, raw_group, true);

        // Read sequences to NACK
        var min_seq: u64 = std.math.maxInt(u64);
        var nack_count: u32 = 0;
        if (reader.readU32()) |count| {
            for (0..count) |_| {
                if (reader.readU64()) |seq| {
                    min_seq = @min(min_seq, seq);
                    nack_count += 1;
                }
            }
        }

        if (group_name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        }

        // NACK resets the group's committed offset to just before the earliest NACK'd message
        // so they will be redelivered on next read.
        if (nack_count > 0 and min_seq > 0) {
            const new_offset = min_seq - 1;
            const group = self.stream.getGroup(group_name) orelse {
                return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
            };
            // Only reset if the new offset is lower than current
            if (new_offset < group.committed_offset) {
                group.committed_offset = new_offset;
            }
        } else {
            // Verify group exists even if no sequences
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

        // Serialize basic group info
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

        const group = self.stream.getGroup(group_name) orelse {
            return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
        };

        // Serialize pending info — for now, return group info format
        // (pending messages = HWM - committed_offset)
        const data = serializeGroupInfo(self.allocator, group) catch {
            return .{ .err = .{ .code = .internal_error, .message = "pending serialization failed" } };
        };

        return .{ .group_pending = .{ .data = data } };
    }

    // ── GROUP TOUCH ─────────────────────────────────────────────────────

    fn handleGroupTouch(self: *StreamHandler, req: Request) CommandResult {
        // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][seq:u64]*
        var reader = WireReader.init(req.value);
        const raw_group = reader.readLengthPrefixed(u16) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

        // Namespace-qualify for isolation (always wire format here)
        var q_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const group_name = resolveGroupName(&q_buf, req.namespace, raw_group, true);
        _ = reader.readLengthPrefixed(u16); // consumer

        var touched_count: u32 = 0;
        if (reader.readU32()) |count| {
            touched_count = count;
            // Skip the actual sequence values — we just count them
            for (0..count) |_| {
                _ = reader.readU64();
            }
        }

        // Verify group exists
        if (self.stream.getGroup(group_name) == null) {
            return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
        }

        // Respond with touched count
        // Wire format: [touched_count:u32]
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, touched_count, .little);
        const data = self.allocator.alloc(u8, 4) catch {
            return .{ .err = .{ .code = .internal_error, .message = "alloc failed" } };
        };
        @memcpy(data, &buf);
        return .{ .group_pending = .{ .data = data } };
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    /// Serialize stream messages with full wire format including payloads.
    ///
    /// Wire format per record (matches CLI parseAndPrintRecords expectations):
    ///   [sequence:u64][timestamp_ms:i64][tier:u8][partition:u32]
    ///   [key_present:u8][payload_len:u32][payload bytes]
    ///   [header_count:u32]
    pub fn serializeMessagesWithPayloads(self: *StreamHandler, entries: []const OffsetEntry, start_offset: u64) ![]u8 {
        // Calculate total size
        var total: usize = 4; // count header
        for (entries) |entry| {
            const result = self.getPayloadAndTier(entry.ual_index);
            // sequence(8) + timestamp_ms(8) + tier(1) + partition(4)
            // + key_present(1) + payload_len(4) + payload + header_count(4)
            total += 8 + 8 + 1 + 4 + 1 + 4 + result.payload.len + 4;
        }

        const buf = try self.allocator.alloc(u8, total);
        errdefer self.allocator.free(buf);

        var pos: usize = 0;

        // Count
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(entries.len), .little);
        pos += 4;

        for (entries, 0..) |entry, i| {
            const result = self.getPayloadAndTier(entry.ual_index);
            const offset = start_offset + i;

            // sequence (1-based offset)
            std.mem.writeInt(u64, buf[pos..][0..8], offset, .little);
            pos += 8;

            // timestamp_ms
            const ts_ms: i64 = @intCast(entry.timestamp_ns / 1_000_000);
            std.mem.writeInt(i64, buf[pos..][0..8], ts_ms, .little);
            pos += 8;

            // tier (0=hot, 1=warm)
            buf[pos] = result.tier;
            pos += 1;

            // partition index from OffsetEntry
            std.mem.writeInt(u32, buf[pos..][0..4], entry.partition_index, .little);
            pos += 4;

            // key_present (0 = no key)
            buf[pos] = 0;
            pos += 1;

            // payload (length-prefixed u32)
            std.mem.writeInt(u32, buf[pos..][0..4], @intCast(result.payload.len), .little);
            pos += 4;
            if (result.payload.len > 0) {
                @memcpy(buf[pos .. pos + result.payload.len], result.payload);
                pos += result.payload.len;
            }

            // header_count (0)
            std.mem.writeInt(u32, buf[pos..][0..4], 0, .little);
            pos += 4;
        }

        return buf;
    }

    /// Public interface for processing pipeline — append payload to stream and return offset.
    pub fn appendPayload(self: *StreamHandler, payload: []const u8) !u64 {
        const timestamp_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
        const next_index = self.partition.ual.max_index + 1;

        // Build command entry with empty key (internal pipeline append)
        const payload_size = entry_mod.COMMAND_PREFIX_SIZE + payload.len;
        const payload_buf = try self.allocator.alloc(u8, payload_size);
        defer self.allocator.free(payload_buf);

        const entry = entry_mod.buildCommandEntry(
            .stream_append,
            entry_mod.Flags.NONE,
            self.partition.current_term,
            next_index,
            timestamp_ns,
            0,
            "", // no key for internal pipeline
            payload,
            payload_buf,
        ) orelse return error.EntryBuildFailed;

        const ual_index = try self.partition.apply(&entry);
        return try self.stream.append(ual_index, timestamp_ns, 0, 0); // 0 = internal pipeline, no stream name
    }

    /// Append payload to a named stream (used by processing pipelines).
    /// Computes the namespace-qualified name hash for proper stream isolation.
    pub fn appendPayloadToStream(self: *StreamHandler, stream_name: []const u8, namespace: []const u8, payload: []const u8) !u64 {
        const timestamp_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
        const next_index = self.partition.ual.max_index + 1;

        // Build command entry with stream name as key
        const payload_size = entry_mod.COMMAND_PREFIX_SIZE + stream_name.len + payload.len;
        const payload_buf = try self.allocator.alloc(u8, payload_size);
        defer self.allocator.free(payload_buf);

        const ns_hash_u32 = router.namespaceHash(namespace);

        const entry = entry_mod.buildCommandEntry(
            .stream_append,
            entry_mod.Flags.NONE,
            self.partition.current_term,
            next_index,
            timestamp_ns,
            ns_hash_u32,
            stream_name,
            payload,
            payload_buf,
        ) orelse return error.EntryBuildFailed;

        const ual_index = try self.partition.apply(&entry);

        // Use the same name hash as handleAppend (namespace-qualified)
        const name_hash = router.nameHash(ns_hash_u32, stream_name);
        const offset = try self.stream.append(ual_index, timestamp_ns, name_hash, 0);

        // Register the stream name so it appears in `stream list`
        // Qualify with namespace prefix like handleAppend does
        var ns_reg_buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const ns_stream_name = ns_keys.qualifyKey(&ns_reg_buf, namespace, stream_name) catch stream_name;
        self.stream.registerStream(ns_stream_name) catch {};

        return offset;
    }

    /// Public interface for processing pipeline — read payloads from offset range.
    /// Returns zero-copy views into the UAL ring buffer. Slices are valid until
    /// the next UAL write that triggers eviction.
    pub fn readPayloads(self: *StreamHandler, start_offset: u64, limit: usize) []const []const u8 {
        var results: [1000][]const u8 = undefined;
        var count: usize = 0;
        const capped = @min(limit, 1000);

        var buf: [1]OffsetEntry = undefined;
        var offset = start_offset;
        while (count < capped) : (offset += 1) {
            const n = self.stream.readRange(offset, offset + 1, &buf);
            if (n == 0) break;
            const result = self.getPayloadAndTier(buf[0].ual_index);
            if (result.payload.len > 0) {
                results[count] = result.payload;
                count += 1;
            } else {
                break; // Entry missing
            }
        }

        // Return a heap-allocated copy of the slice
        const out = self.allocator.alloc([]const u8, count) catch return &.{};
        @memcpy(out, results[0..count]);
        return out;
    }

    /// Read payloads from a named stream (used by processing pipelines).
    /// Only returns entries belonging to the specified stream.
    /// Returns (payloads slice, last_global_offset scanned) so the pipeline
    /// can advance its cursor past all scanned offsets.
    pub fn readPayloadsForStream(self: *StreamHandler, stream_name: []const u8, namespace: []const u8, start_offset: u64, limit: usize) struct { payloads: []const []const u8, next_offset: u64 } {
        var results: [1000][]const u8 = undefined;
        var count: usize = 0;
        const capped = @min(limit, 1000);

        const ns_hash_u32 = router.namespaceHash(namespace);
        const name_hash = router.nameHash(ns_hash_u32, stream_name);
        var buf: [1000]OffsetEntry = undefined;
        const batch = @min(capped, 1000);

        // Scan global offsets from start, filtering by name hash.
        // readRangeForStream returns only matching entries.
        const hwm = self.stream.highWaterMark();
        var end_offset = start_offset + batch * 10; // scan well ahead to find matching entries
        if (end_offset < hwm + 1) end_offset = hwm + 1;

        const n = self.stream.readRangeForStream(start_offset, end_offset, name_hash, buf[0..batch]);

        var last_scanned: u64 = start_offset;
        for (buf[0..n]) |oe| {
            if (count >= capped) break;
            const result = self.getPayloadAndTier(oe.ual_index);
            if (result.payload.len > 0) {
                results[count] = result.payload;
                count += 1;
            }
        }

        // We need to know the last global offset we've scanned to advance cursor.
        // Since readRangeForStream scans up to end_offset, advance past hwm.
        if (n > 0) {
            // The entries are in offset order; find the actual offsets by scanning
            // the same range again to get the global offset of the last entry.
            // But since the offsets map is keyed by global offset, we can use hwm.
            last_scanned = @min(end_offset, hwm + 1);
        } else {
            last_scanned = @min(end_offset, hwm + 1);
        }

        const out = self.allocator.alloc([]const u8, count) catch return .{ .payloads = &.{}, .next_offset = last_scanned };
        @memcpy(out, results[0..count]);
        return .{ .payloads = out, .next_offset = last_scanned };
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
            // Serialize: [first_ts:u64][first_seq:u64][last_ts:u64][last_seq:u64][count:u64][bytes:u64][partition_count:u32]
            var buf: [52]u8 = undefined;
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

/// Serialize offset entries to binary format.
/// Wire format: [count:u32] ([offset:u64][ual_index:u64][timestamp_ns:u64])*
fn serializeOffsetEntries(allocator: Allocator, entries: []const OffsetEntry) ![]u8 {
    return serializeOffsetEntriesPub(allocator, entries);
}

/// Public variant of serializeOffsetEntries — used by the WaiterPool resolver
/// in shard.zig to build stream responses for blocking reads.
pub fn serializeOffsetEntriesPub(allocator: Allocator, entries: []const OffsetEntry) ![]u8 {
    const entry_size: usize = 24; // 3 x u64
    const total = 4 + entries.len * entry_size;

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var offset: usize = 0;

    // Count
    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(entries.len), .little);
    offset += 4;

    for (entries, 0..) |entry, i| {
        // Offset (1-based index)
        const stream_offset = i + 1; // offsets are sequential
        std.mem.writeInt(u64, buf[offset..][0..8], @intCast(stream_offset), .little);
        offset += 8;

        // UAL index
        std.mem.writeInt(u64, buf[offset..][0..8], entry.ual_index, .little);
        offset += 8;

        // Timestamp
        std.mem.writeInt(u64, buf[offset..][0..8], entry.timestamp_ns, .little);
        offset += 8;
    }

    return buf;
}

/// Serialize consumer group info.
/// Wire format: [committed_offset:u64][member_count:u32][created_at_ns:u64]
fn serializeGroupInfo(allocator: Allocator, group: *stream_mod.ConsumerGroup) ![]u8 {
    const total: usize = 8 + 4 + 8; // committed_offset + member_count + created_at_ns
    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var offset: usize = 0;

    std.mem.writeInt(u64, buf[offset..][0..8], group.committed_offset, .little);
    offset += 8;

    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(group.memberCount()), .little);
    offset += 4;

    std.mem.writeInt(u64, buf[offset..][0..8], group.created_at_ns, .little);

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
            .reserved = 0,
        },
        .namespace = "default",
        .key = key,
        .value = value,
        .options = options,
    };
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

    try testing.expectEqual(@as(u16, 16), dispatcher.handler_count);
}

test "stream handler: append" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.stream_append, "events", "payload1", ""));
    switch (result) {
        .stream_append_ok => |a| {
            try testing.expectEqual(@as(u64, 1), a.sequence);
            try testing.expect(a.timestamp_ms > 0);
        },
        else => return error.TestUnexpectedResult,
    }

    // Second append
    const r2 = handler.handleCommand(makeRequest(.stream_append, "events", "payload2", ""));
    switch (r2) {
        .stream_append_ok => |a| try testing.expectEqual(@as(u64, 2), a.sequence),
        else => return error.TestUnexpectedResult,
    }

    // HWM should be 2
    try testing.expectEqual(@as(u64, 2), partition.stream.highWaterMark());
}

test "stream handler: append empty stream name" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();
    const result = handler.handleCommand(makeRequest(.stream_append, "", "data", ""));
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
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "a", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "b", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "c", ""));

    // Read all
    const result = handler.handleCommand(makeRequest(.stream_read, "s1", "", ""));
    switch (result) {
        .stream_messages => |m| {
            defer handler.freeResult(result);
            // Parse count from serialized data
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 3), count);
            try testing.expectEqual(@as(u64, 4), m.next_sequence); // 1 + 3
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
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "1", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "2", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "3", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "4", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "5", ""));

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
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "a", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "b", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "c", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "d", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "e", ""));

    // Trim up to offset 3 (pass as value)
    const result = handler.handleCommand(makeRequest(.stream_trim, "s1", "3", ""));
    switch (result) {
        .stream_trimmed => |t| {
            try testing.expectEqual(@as(u64, 3), t.deleted_count);
            try testing.expectEqual(@as(u64, 4), t.first_seq);
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
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "a", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "b", ""));

    const result = handler.handleCommand(makeRequest(.stream_info, "s1", "", ""));
    switch (result) {
        .stream_info => |info| {
            try testing.expectEqual(@as(u64, 2), info.last_seq);
            try testing.expectEqual(@as(u64, 2), info.count);
            try testing.expectEqual(@as(u32, 1), info.partition_count);
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
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "msg1", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "msg2", ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", "msg3", ""));

    // Create group
    _ = handler.handleCommand(makeRequest(.stream_group_create, "s1", "cg1", ""));

    // Read from group (starts at offset 1 since committed_offset is 0)
    const read_result = handler.handleCommand(makeRequest(.stream_group_read, "s1", "cg1", ""));
    switch (read_result) {
        .group_messages => |m| {
            defer handler.freeResult(read_result);
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 3), count);
        },
        else => return error.TestUnexpectedResult,
    }

    // Ack up to offset 2 (namespace="2" for offset)
    var ack_req = makeRequest(.stream_group_ack, "s1", "cg1", "");
    ack_req.namespace = "2";
    const ack_result = handler.handleCommand(ack_req);
    switch (ack_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }

    // Read again — should start from offset 3
    const read2_result = handler.handleCommand(makeRequest(.stream_group_read, "s1", "cg1", ""));
    switch (read2_result) {
        .group_messages => |m| {
            defer handler.freeResult(read2_result);
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 1), count); // only msg3
        },
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

    // Ack non-existent group (use a different name than "nope" which was auto-created above)
    var ack_req = makeRequest(.stream_group_ack, "s1", "no_such_group", "");
    ack_req.namespace = "1";
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
