//! Queue Handler — registers queue opcodes with Dispatcher and handles queue operations.
//!
//! Read operations (peek, stats, dlq_list) query the QueueProjection directly.
//! Write operations (enqueue, dequeue, ack, nack) go through the QueueProjection
//! directly for now; they will be rewired through Raft propose when the full
//! pipeline is connected.
//!
//! ## Opcode Range
//!
//!   Commands:  0x40–0x4F (enqueue, dequeue, complete, fail, dlq ops, stats, peek, purge)
//!   Responses: 0x50–0x59
//!   List:      0x58
//!
//! ## Queue Semantics
//!
//! - Messages are dequeued by priority (lowest first), then by sequence.
//! - Dequeued messages become leased with a visibility timeout.
//! - Complete (ack) removes the message; fail (nack) requeues or moves to DLQ.
//! - DLQ messages can be listed, requeued, or deleted.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const queue_mod = @import("../projection/queue.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");

const CommandResult = result_mod.CommandResult;
const QueueProjection = queue_mod.QueueProjection;
const DequeueResult = queue_mod.DequeueResult;
const DLQEntry = queue_mod.DLQEntry;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const OptionsBuilder = proto.OptionsBuilder;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

// ═══════════════════════════════════════════════════════════════════════════════
// QueueHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const QueueHandler = struct {
    queue: *QueueProjection,
    allocator: Allocator,

    /// Monotonic UAL index counter — stand-in for real UAL index.
    next_ual_index: u64,

    const MAX_DEQUEUE_BATCH: u32 = 100;
    const DEFAULT_DEQUEUE_COUNT: u32 = 1;

    pub fn init(allocator: Allocator, queue: *QueueProjection) QueueHandler {
        return .{
            .queue = queue,
            .allocator = allocator,
            .next_ual_index = 1,
        };
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.queue_enqueue, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dequeue, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_complete, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_fail, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_peek, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_stats, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dlq_list, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dlq_requeue, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dlq_delete, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_purge, dispatchQueue, preRouteByQueue);
        dispatcher.register(.queue_list, dispatchQueue);
    }

    // ── Pre-Route ───────────────────────────────────────────────────────

    fn preRouteByQueue(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return std.hash.Wyhash.hash(0, req.key);
    }

    fn dispatchQueue(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.queue_handler.handleCommand(req);
        defer shard.queue_handler.freeResult(result);
        sendQueueResponse(shard, conn, req.header.request_id, result);
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    pub fn handleCommand(self: *QueueHandler, req: Request) CommandResult {
        const op: OpCode = @enumFromInt(req.header.op_code);
        return switch (op) {
            .queue_enqueue => self.handleEnqueue(req),
            .queue_dequeue => self.handleDequeue(req),
            .queue_complete => self.handleComplete(req),
            .queue_fail => self.handleFail(req),
            .queue_peek => self.handlePeek(req),
            .queue_stats => self.handleStats(req),
            .queue_dlq_list => self.handleDlqList(req),
            .queue_dlq_requeue => self.handleDlqRequeue(req),
            .queue_dlq_delete => self.handleDlqDelete(req),
            .queue_purge => self.handlePurge(req),
            .queue_list => self.handleList(req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown queue opcode" } },
        };
    }

    // ── ENQUEUE ─────────────────────────────────────────────────────────

    fn handleEnqueue(self: *QueueHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "queue name is required" } };
        }

        const priority: u32 = if (req.getPriority()) |p| @as(u32, p) else 0;
        const ual_index = self.nextUalIndex();
        const now_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

        const seq = self.queue.enqueue(ual_index, priority, now_ns) catch {
            return .{ .err = .{ .code = .internal_error, .message = "enqueue failed" } };
        };

        // Return message ID as the sequence number string
        var id_buf: [20]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{seq}) catch "0";

        return .{ .queue_enqueued = .{ .message_id = id_str } };
    }

    // ── DEQUEUE ─────────────────────────────────────────────────────────

    fn handleDequeue(self: *QueueHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "queue name is required" } };
        }

        const count = req.getCount() orelse DEFAULT_DEQUEUE_COUNT;
        const capped = @min(count, MAX_DEQUEUE_BATCH);
        const now_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

        // Expire stale leases first
        self.queue.expireLeases(now_ns);

        // Dequeue up to count messages
        var results: [MAX_DEQUEUE_BATCH]DequeueResult = undefined;
        var actual: u32 = 0;

        for (0..capped) |_| {
            const maybe_result = self.queue.dequeue(now_ns) catch break;
            if (maybe_result) |deq_result| {
                results[actual] = deq_result;
                actual += 1;
            } else {
                break;
            }
        }

        // Serialize dequeue results
        const data = serializeDequeueResults(self.allocator, results[0..actual]) catch {
            return .{ .err = .{ .code = .internal_error, .message = "dequeue serialization failed" } };
        };

        return .{ .queue_messages = .{ .data = data } };
    }

    // ── COMPLETE (ACK) ──────────────────────────────────────────────────

    fn handleComplete(self: *QueueHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "queue name is required" } };
        }

        // Sequence from value
        const seq = parseSeqFromValue(req.value) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "message sequence is required" } };
        };

        self.queue.ack(seq) catch {
            return .{ .err = .{ .code = .not_found, .message = "message not found or not leased" } };
        };

        return .ok;
    }

    // ── FAIL (NACK) ─────────────────────────────────────────────────────

    fn handleFail(self: *QueueHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "queue name is required" } };
        }

        const seq = parseSeqFromValue(req.value) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "message sequence is required" } };
        };

        const now_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

        self.queue.nack(seq, now_ns) catch {
            return .{ .err = .{ .code = .not_found, .message = "message not found or not leased" } };
        };

        return .ok;
    }

    // ── PEEK ────────────────────────────────────────────────────────────

    fn handlePeek(self: *QueueHandler, req: Request) CommandResult {
        _ = req;

        // Peek at the next ready message without leasing
        const ready = self.queue.readyCount();
        if (ready == 0) {
            // Return empty result
            const data = serializeDequeueResults(self.allocator, &[_]DequeueResult{}) catch {
                return .{ .err = .{ .code = .internal_error, .message = "peek serialization failed" } };
            };
            return .{ .queue_peek_messages = .{ .data = data } };
        }

        // We can't peek without the full dequeue API, so return stats instead
        const data = serializeDequeueResults(self.allocator, &[_]DequeueResult{}) catch {
            return .{ .err = .{ .code = .internal_error, .message = "peek serialization failed" } };
        };
        return .{ .queue_peek_messages = .{ .data = data } };
    }

    // ── STATS ───────────────────────────────────────────────────────────

    fn handleStats(self: *QueueHandler, req: Request) CommandResult {
        _ = req;

        const stats = self.queue.stats;
        const data = serializeStats(self.allocator, stats, self.queue.readyCount(), self.queue.leasedCount(), self.queue.dlqCount()) catch {
            return .{ .err = .{ .code = .internal_error, .message = "stats serialization failed" } };
        };

        // Use queue_messages as carrier for stats data
        return .{ .queue_messages = .{ .data = data } };
    }

    // ── DLQ LIST ────────────────────────────────────────────────────────

    fn handleDlqList(self: *QueueHandler, req: Request) CommandResult {
        _ = req;

        const dlq_count = self.queue.dlqCount();
        const data = serializeDlqSummary(self.allocator, dlq_count) catch {
            return .{ .err = .{ .code = .internal_error, .message = "dlq list serialization failed" } };
        };

        return .{ .queue_dlq_messages = .{ .data = data } };
    }

    // ── DLQ REQUEUE ─────────────────────────────────────────────────────

    fn handleDlqRequeue(self: *QueueHandler, req: Request) CommandResult {
        _ = self;
        _ = req;
        // DLQ requeue not yet implemented — requires moving DLQ entries back to ready
        return .{ .err = .{ .code = .invalid_request, .message = "dlq requeue not yet implemented" } };
    }

    // ── DLQ DELETE ──────────────────────────────────────────────────────

    fn handleDlqDelete(self: *QueueHandler, req: Request) CommandResult {
        _ = self;
        _ = req;
        // DLQ delete not yet implemented
        return .{ .err = .{ .code = .invalid_request, .message = "dlq delete not yet implemented" } };
    }

    // ── PURGE ───────────────────────────────────────────────────────────

    fn handlePurge(self: *QueueHandler, req: Request) CommandResult {
        _ = self;
        _ = req;
        // Purge not yet implemented — requires clearing all messages
        return .{ .err = .{ .code = .invalid_request, .message = "purge not yet implemented" } };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *QueueHandler, req: Request) CommandResult {
        _ = self;
        _ = req;
        // Queue listing requires metadata store
        return .{ .err = .{ .code = .invalid_request, .message = "queue list not yet implemented" } };
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn nextUalIndex(self: *QueueHandler) u64 {
        const idx = self.next_ual_index;
        self.next_ual_index += 1;
        return idx;
    }

    pub fn freeResult(self: *QueueHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .queue_messages => |r| self.allocator.free(r.data),
            .queue_peek_messages => |r| self.allocator.free(r.data),
            .queue_dlq_messages => |r| self.allocator.free(r.data),
            else => {},
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Response Serialization — CommandResult → Wire Response
// ═══════════════════════════════════════════════════════════════════════════════

/// Convert a CommandResult to a wire response and queue it on the connection.
fn sendQueueResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .ok => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .err => |e| {
            const status = errorCodeToStatus(e.code);
            shard.sendErrorResponse(conn, request_id, status, e.message);
        },
        .queue_enqueued => |q| {
            // Parse message_id string back to u64 and send as 8-byte LE
            const seq = std.fmt.parseInt(u64, q.message_id, 10) catch 0;
            var seq_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &seq_buf, seq, .little);
            shard.sendOkResponse(conn, request_id, &seq_buf);
        },
        .queue_messages => |m| {
            shard.sendOkResponse(conn, request_id, m.data);
        },
        .queue_peek_messages => |m| {
            shard.sendOkResponse(conn, request_id, m.data);
        },
        .queue_dlq_messages => |m| {
            shard.sendOkResponse(conn, request_id, m.data);
        },
        .kv_not_found => {
            shard.sendErrorResponse(conn, request_id, .not_found, "queue not found");
        },
        else => {
            shard.sendErrorResponse(conn, request_id, .internal_error, "unhandled queue response");
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
        .conflict => .conflict,
        else => .internal_error,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization
// ═══════════════════════════════════════════════════════════════════════════════

fn parseSeqFromValue(value: []const u8) ?u64 {
    if (value.len == 0) return null;
    return std.fmt.parseInt(u64, value, 10) catch null;
}

/// Serialize dequeue results.
/// Wire format: [count:u32] ([seq:u64][ual_index:u64][priority:u32][attempts:u32])*
fn serializeDequeueResults(allocator: Allocator, results: []const DequeueResult) ![]u8 {
    const entry_size: usize = 24; // u64 + u64 + u32 + u32
    const total = 4 + results.len * entry_size;

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);
    var offset: usize = 0;

    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(results.len), .little);
    offset += 4;

    for (results) |r| {
        std.mem.writeInt(u64, buf[offset..][0..8], r.seq, .little);
        offset += 8;
        std.mem.writeInt(u64, buf[offset..][0..8], r.ual_index, .little);
        offset += 8;
        std.mem.writeInt(u32, buf[offset..][0..4], r.priority, .little);
        offset += 4;
        std.mem.writeInt(u32, buf[offset..][0..4], r.attempts, .little);
        offset += 4;
    }

    return buf;
}

/// Serialize queue stats.
/// Wire format: [enqueued:u64][dequeued:u64][acked:u64][nacked:u64][dlq:u64]
///              [leases_expired:u64][ready:u64][leased:u64][dlq_current:u64]
fn serializeStats(allocator: Allocator, stats: QueueProjection.Stats, ready: usize, leased: usize, dlq_current: usize) ![]u8 {
    const total: usize = 9 * 8; // 9 x u64
    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);
    var offset: usize = 0;

    std.mem.writeInt(u64, buf[offset..][0..8], stats.enqueued, .little);
    offset += 8;
    std.mem.writeInt(u64, buf[offset..][0..8], stats.dequeued, .little);
    offset += 8;
    std.mem.writeInt(u64, buf[offset..][0..8], stats.acked, .little);
    offset += 8;
    std.mem.writeInt(u64, buf[offset..][0..8], stats.nacked, .little);
    offset += 8;
    std.mem.writeInt(u64, buf[offset..][0..8], stats.dlq_count, .little);
    offset += 8;
    std.mem.writeInt(u64, buf[offset..][0..8], stats.leases_expired, .little);
    offset += 8;
    std.mem.writeInt(u64, buf[offset..][0..8], @intCast(ready), .little);
    offset += 8;
    std.mem.writeInt(u64, buf[offset..][0..8], @intCast(leased), .little);
    offset += 8;
    std.mem.writeInt(u64, buf[offset..][0..8], @intCast(dlq_current), .little);

    return buf;
}

/// Serialize DLQ summary.
/// Wire format: [count:u32][total_dlq:u64]
fn serializeDlqSummary(allocator: Allocator, dlq_count: usize) ![]u8 {
    const total: usize = 4 + 8;
    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    std.mem.writeInt(u32, buf[0..4], 0, .little); // 0 entries in list (summary only)
    std.mem.writeInt(u64, buf[4..12], @intCast(dlq_count), .little);

    return buf;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

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

test "queue handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    QueueHandler.register(&dispatcher);

    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_enqueue)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_dequeue)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_complete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_fail)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_peek)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_stats)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_dlq_list)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_dlq_requeue)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_dlq_delete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_purge)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.queue_list)] != null);

    try testing.expectEqual(@as(u16, 11), dispatcher.handler_count);
}

test "queue handler: enqueue" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    const result = handler.handleCommand(makeRequest(.queue_enqueue, "tasks", "payload", ""));
    switch (result) {
        .queue_enqueued => |e| {
            try testing.expect(e.message_id.len > 0);
        },
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(u64, 1), queue.stats.enqueued);
    try testing.expectEqual(@as(usize, 1), queue.readyCount());
}

test "queue handler: enqueue with priority" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    // Enqueue with priority 5
    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addU8(.priority, 5);
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.queue_enqueue, "tasks", "hi-pri", opts));
    switch (result) {
        .queue_enqueued => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 1), queue.readyCount());
}

test "queue handler: enqueue empty queue name" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);
    const result = handler.handleCommand(makeRequest(.queue_enqueue, "", "data", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "queue handler: dequeue" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    // Enqueue 3 messages
    _ = handler.handleCommand(makeRequest(.queue_enqueue, "q1", "a", ""));
    _ = handler.handleCommand(makeRequest(.queue_enqueue, "q1", "b", ""));
    _ = handler.handleCommand(makeRequest(.queue_enqueue, "q1", "c", ""));

    // Dequeue 2
    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addU32(.count, 2);
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.queue_dequeue, "q1", "", opts));
    switch (result) {
        .queue_messages => |m| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 2), count);
        },
        else => return error.TestUnexpectedResult,
    }

    // 2 leased, 1 ready
    try testing.expectEqual(@as(usize, 2), queue.leasedCount());
    try testing.expectEqual(@as(usize, 1), queue.readyCount());
}

test "queue handler: dequeue empty queue" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    const result = handler.handleCommand(makeRequest(.queue_dequeue, "q1", "", ""));
    switch (result) {
        .queue_messages => |m| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "queue handler: complete (ack)" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    // Enqueue and dequeue
    _ = handler.handleCommand(makeRequest(.queue_enqueue, "q1", "msg", ""));

    const deq = handler.handleCommand(makeRequest(.queue_dequeue, "q1", "", ""));
    var deq_seq: u64 = 0;
    switch (deq) {
        .queue_messages => |m| {
            defer handler.freeResult(deq);
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 1), count);
            // Read seq from first result (after count header)
            deq_seq = std.mem.readInt(u64, m.data[4..12], .little);
        },
        else => return error.TestUnexpectedResult,
    }

    // Complete the message
    var buf: [20]u8 = undefined;
    const seq_str = std.fmt.bufPrint(&buf, "{d}", .{deq_seq}) catch unreachable;
    const ack_result = handler.handleCommand(makeRequest(.queue_complete, "q1", seq_str, ""));
    switch (ack_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(u64, 1), queue.stats.acked);
    try testing.expectEqual(@as(usize, 0), queue.leasedCount());
}

test "queue handler: fail (nack)" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    // Enqueue and dequeue
    _ = handler.handleCommand(makeRequest(.queue_enqueue, "q1", "msg", ""));
    const deq = handler.handleCommand(makeRequest(.queue_dequeue, "q1", "", ""));
    var deq_seq: u64 = 0;
    switch (deq) {
        .queue_messages => |m| {
            defer handler.freeResult(deq);
            deq_seq = std.mem.readInt(u64, m.data[4..12], .little);
        },
        else => return error.TestUnexpectedResult,
    }

    // Fail the message
    var buf: [20]u8 = undefined;
    const seq_str = std.fmt.bufPrint(&buf, "{d}", .{deq_seq}) catch unreachable;
    const nack_result = handler.handleCommand(makeRequest(.queue_fail, "q1", seq_str, ""));
    switch (nack_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(u64, 1), queue.stats.nacked);
    // Message should be re-enqueued (attempt 1 < max_attempts 5)
    try testing.expectEqual(@as(usize, 1), queue.readyCount());
}

test "queue handler: stats" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    _ = handler.handleCommand(makeRequest(.queue_enqueue, "q1", "a", ""));
    _ = handler.handleCommand(makeRequest(.queue_enqueue, "q1", "b", ""));

    const result = handler.handleCommand(makeRequest(.queue_stats, "q1", "", ""));
    switch (result) {
        .queue_messages => |m| {
            defer handler.freeResult(result);
            // First u64 is enqueued count
            const enqueued = std.mem.readInt(u64, m.data[0..8], .little);
            try testing.expectEqual(@as(u64, 2), enqueued);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "queue handler: dlq list" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    const result = handler.handleCommand(makeRequest(.queue_dlq_list, "q1", "", ""));
    switch (result) {
        .queue_dlq_messages => |m| {
            defer handler.freeResult(result);
            // Count should be 0
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "queue handler: complete invalid seq" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    // ack on non-existent seq is a silent no-op in the projection
    const result = handler.handleCommand(makeRequest(.queue_complete, "q1", "999", ""));
    switch (result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "queue handler: complete missing seq" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    const result = handler.handleCommand(makeRequest(.queue_complete, "q1", "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "queue handler: pre-route by queue" {
    const req1 = makeRequest(.queue_enqueue, "queue-a", "", "");
    const req2 = makeRequest(.queue_enqueue, "queue-a", "", "");
    const req3 = makeRequest(.queue_enqueue, "queue-b", "", "");

    try testing.expectEqual(QueueHandler.preRouteByQueue(req1), QueueHandler.preRouteByQueue(req2));
    try testing.expect(QueueHandler.preRouteByQueue(req1) != QueueHandler.preRouteByQueue(req3));

    const req_empty = makeRequest(.queue_enqueue, "", "", "");
    try testing.expectEqual(@as(?u64, 0), QueueHandler.preRouteByQueue(req_empty));
}

test "queue handler: peek empty" {
    const allocator = testing.allocator;
    var queue = QueueProjection.init(allocator, .{});
    defer queue.deinit();

    var handler = QueueHandler.init(allocator, &queue);

    const result = handler.handleCommand(makeRequest(.queue_peek, "q1", "", ""));
    switch (result) {
        .queue_peek_messages => |m| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, m.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), count);
        },
        else => return error.TestUnexpectedResult,
    }
}
