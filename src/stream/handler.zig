//! Stream Handler — registers stream and consumer-group opcodes with Dispatcher.
//!
//! Read operations (read, info, group_pending) query the StreamProjection directly.
//! Write operations (append, trim, group create/join/leave/ack) go through the
//! StreamProjection directly for now; they will be rewired through Raft propose
//! when the full pipeline is connected.
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
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

const CommandResult = result_mod.CommandResult;
const StreamProjection = stream_mod.StreamProjection;
const OffsetEntry = stream_mod.OffsetEntry;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const OptionsBuilder = proto.OptionsBuilder;
const waiter_pool_mod = @import("../node/waiter_pool.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// StreamHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const StreamHandler = struct {
    stream: *StreamProjection,
    allocator: Allocator,

    /// Monotonic UAL index counter — stand-in for real UAL index until full pipeline.
    next_ual_index: u64,

    /// Maximum number of messages in a single read response.
    const MAX_READ_BATCH: usize = 1000;
    const DEFAULT_READ_BATCH: usize = 100;

    pub fn init(allocator: Allocator, stream: *StreamProjection) StreamHandler {
        return .{
            .stream = stream,
            .allocator = allocator,
            .next_ual_index = 1,
        };
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    /// Register stream and consumer-group opcode handlers with the Dispatcher.
    pub fn register(dispatcher: *Dispatcher) void {
        // Stream operations (0x10–0x1F)
        dispatcher.registerWithRoute(.stream_append, dispatchAppend, preRouteByStream);
        dispatcher.registerWithRoute(.stream_read, dispatchRead, preRouteByStream);
        dispatcher.registerWithRoute(.stream_trim, dispatchStream, preRouteByStream);
        dispatcher.registerWithRoute(.stream_info, dispatchStream, preRouteByStream);
        dispatcher.register(.stream_list, dispatchStream);
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
    }

    // ── Pre-Route Hooks ─────────────────────────────────────────────────

    /// Route by stream name — single-partition operations.
    fn preRouteByStream(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return std.hash.Wyhash.hash(0, req.key);
    }

    // ── Dispatch Wrappers ───────────────────────────────────────────────

    fn dispatchStream(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.stream_handler.handleCommand(req);
        defer shard.stream_handler.freeResult(cmd_result);
        sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
    }

    /// Dedicated dispatch for stream_append — notifies blocking read waiters after append.
    fn dispatchAppend(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.stream_handler.handleCommand(req);
        defer shard.stream_handler.freeResult(cmd_result);

        // After a successful append, notify any blocking read waiters
        switch (cmd_result) {
            .stream_append_ok => {
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
            const hwm = shard.stream_projection.highWaterMark();
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

            else => .{ .err = .{ .code = .invalid_request, .message = "unknown stream opcode" } },
        };
    }

    // ── APPEND ──────────────────────────────────────────────────────────

    fn handleAppend(self: *StreamHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "stream name is required" } };
        }

        // TODO: Replace with Raft propose; actual payload stored in UAL.
        const ual_index = self.nextUalIndex();
        const timestamp_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

        const offset = self.stream.append(ual_index, timestamp_ns) catch {
            return .{ .err = .{ .code = .internal_error, .message = "append failed" } };
        };

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

        // Check for tail flag
        if (req.findOption(.stream_tail) != null) {
            const hwm = self.stream.highWaterMark();
            start_offset = if (hwm > 0) hwm else 1;
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

        // Read the range
        var buf: [MAX_READ_BATCH]OffsetEntry = undefined;
        const out = buf[0..capped];
        const count = self.stream.readRange(start_offset, end_offset, out);

        // Serialize the offset entries
        const data = serializeOffsetEntries(self.allocator, out[0..count]) catch {
            return .{ .err = .{ .code = .internal_error, .message = "read serialization failed" } };
        };

        const next_ts: u64 = if (count > 0) out[count - 1].timestamp_ns / 1_000_000 else 0;
        const next_seq: u64 = start_offset + count;

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
        _ = req;

        const hwm = self.stream.highWaterMark();
        const count = self.stream.trackedOffsets();

        return .{ .stream_info = .{
            .first_timestamp_ms = 0,
            .first_seq = if (count > 0) @as(u64, 1) else 0,
            .last_timestamp_ms = 0,
            .last_seq = hwm,
            .count = count,
            .bytes = 0, // Not tracked by projection
            .partition_count = 1,
        } };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *StreamHandler, req: Request) CommandResult {
        _ = self;
        _ = req;
        // Stream listing requires metadata store (not in StreamProjection)
        return .{ .err = .{ .code = .invalid_request, .message = "stream list not yet implemented" } };
    }

    // ── CREATE ──────────────────────────────────────────────────────────

    fn handleCreate(self: *StreamHandler, req: Request) CommandResult {
        _ = self;
        _ = req;
        // Stream creation requires metadata store
        // For now, streams are implicit (created on first append)
        return .ok;
    }

    // ── GROUP CREATE ────────────────────────────────────────────────────

    fn handleGroupCreate(self: *StreamHandler, req: Request) CommandResult {
        // Group name from value, stream from key
        const group_name = if (req.value.len > 0) req.value else {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

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
        const group_name = if (req.value.len > 0) req.value else {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

        if (self.stream.deleteGroup(group_name)) {
            return .ok;
        } else {
            return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
        }
    }

    // ── GROUP JOIN ──────────────────────────────────────────────────────

    fn handleGroupJoin(self: *StreamHandler, req: Request) CommandResult {
        // value = group name, namespace used for member ID or use a default
        const group_name = if (req.value.len > 0) req.value else {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

        // Member ID from options or generate from namespace
        const member_id = if (req.namespace.len > 0) req.namespace else "default";
        const now_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

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
        const group_name = if (req.value.len > 0) req.value else {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

        const member_id = if (req.namespace.len > 0) req.namespace else "default";

        _ = self.stream.leaveGroup(group_name, member_id) catch |err| {
            return switch (err) {
                error.GroupNotFound => .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } },
            };
        };

        return .ok;
    }

    // ── GROUP READ ──────────────────────────────────────────────────────

    fn handleGroupRead(self: *StreamHandler, req: Request) CommandResult {
        const group_name = if (req.value.len > 0) req.value else {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

        const group = self.stream.getGroup(group_name) orelse {
            return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
        };

        const limit = req.getLimit() orelse DEFAULT_READ_BATCH;
        const capped = @min(limit, MAX_READ_BATCH);

        // Read from group's committed offset
        const start = group.committed_offset + 1;
        const end = start + capped;

        var buf: [MAX_READ_BATCH]OffsetEntry = undefined;
        const count = self.stream.readRange(start, end, buf[0..capped]);

        const data = serializeOffsetEntries(self.allocator, buf[0..count]) catch {
            return .{ .err = .{ .code = .internal_error, .message = "group read serialization failed" } };
        };

        return .{ .group_messages = .{ .data = data } };
    }

    // ── GROUP ACK ───────────────────────────────────────────────────────

    fn handleGroupAck(self: *StreamHandler, req: Request) CommandResult {
        const group_name = if (req.value.len > 0) req.value else {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

        // Parse offset from options (stream_start) or from a simple integer in namespace
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
        _ = self;
        _ = req;
        // NACK redelivery not yet implemented
        return .{ .err = .{ .code = .invalid_request, .message = "nack not yet implemented" } };
    }

    // ── GROUP INFO ──────────────────────────────────────────────────────

    fn handleGroupInfo(self: *StreamHandler, req: Request) CommandResult {
        const group_name = if (req.value.len > 0) req.value else {
            return .{ .err = .{ .code = .invalid_request, .message = "group name is required" } };
        };

        const group = self.stream.getGroup(group_name) orelse {
            return .{ .err = .{ .code = .group_not_found, .message = "consumer group not found" } };
        };

        // Serialize basic group info
        const data = serializeGroupInfo(self.allocator, group) catch {
            return .{ .err = .{ .code = .internal_error, .message = "group info serialization failed" } };
        };

        return .{ .group_pending = .{ .data = data } };
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn nextUalIndex(self: *StreamHandler) u64 {
        const idx = self.next_ual_index;
        self.next_ual_index += 1;
        return idx;
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
            // Send sequence as 8-byte u64 LE
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, a.sequence, .little);
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

    try testing.expectEqual(@as(u16, 14), dispatcher.handler_count);
}

test "stream handler: append" {
    const allocator = testing.allocator;
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);

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
    try testing.expectEqual(@as(u64, 2), stream.highWaterMark());
}

test "stream handler: append empty stream name" {
    const allocator = testing.allocator;
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);
    const result = handler.handleCommand(makeRequest(.stream_append, "", "data", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: read" {
    const allocator = testing.allocator;
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);

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
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);

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
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);

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
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);

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
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);
    const result = handler.handleCommand(makeRequest(.stream_create, "new_stream", "", ""));
    switch (result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: group lifecycle" {
    const allocator = testing.allocator;
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);

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
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);

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
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);

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
    var stream = StreamProjection.init(allocator);
    defer stream.deinit();

    var handler = StreamHandler.init(allocator, &stream);

    // Join non-existent group
    const join_result = handler.handleCommand(makeRequest(.stream_group_join, "s1", "nope", ""));
    switch (join_result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.group_not_found, e.code),
        else => return error.TestUnexpectedResult,
    }

    // Ack non-existent group
    var ack_req = makeRequest(.stream_group_ack, "s1", "nope", "");
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
}
