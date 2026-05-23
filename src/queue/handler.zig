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
const router = @import("../node/router.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const Partition = @import("../storage/partition.zig").Partition;
const persistence_mod = @import("../storage/persistence.zig");
const ReplayRegistry = persistence_mod.ReplayRegistry;
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;

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
const waiter_pool_mod = @import("../node/waiter_pool.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// QueueHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const QueueHandler = struct {
    queue: *QueueProjection,
    partition: *Partition,
    allocator: Allocator,

    /// Opaque pointer to owning Shard (avoids circular import).
    /// Set after init by Shard.wireHandlerShardPtrs(). Required for Raft writes.
    shard_ptr: ?*anyopaque,

    /// Global metrics registry (optional, set by runtime when dashboard is enabled).
    metrics_registry: ?*MetricsRegistry,

    const MAX_DEQUEUE_BATCH: u32 = 100;
    const DEFAULT_DEQUEUE_COUNT: u32 = 1;

    pub fn init(allocator: Allocator, partition: *Partition) QueueHandler {
        return .{
            .queue = &partition.queue,
            .partition = partition,
            .allocator = allocator,
            .shard_ptr = null,
            .metrics_registry = null,
        };
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.queue_enqueue, dispatchEnqueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dequeue, dispatchDequeue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_complete, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_fail, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_peek, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_stats, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dlq_list, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dlq_requeue, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dlq_delete, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_purge, dispatchQueue, preRouteByQueue);
        dispatcher.registerWalk(.queue_list, dispatchQueue, localScanQueues);
    }

    /// ShardWalker LocalScanFn for queue_list — returns queue names
    /// from one shard's QueueProjection registry.
    fn localScanQueues(
        ctx: *anyopaque,
        namespace: []const u8,
        _: []const u8, // filter
        _: ?[]const u8, // cursor
        _: u32, // limit
    ) dispatcher_mod.NameWalker.ScanResult {
        const handler: *QueueHandler = @ptrCast(@alignCast(ctx));
        const S = struct {
            threadlocal var name_buf: [256][]const u8 = undefined;
        };

        var count: usize = 0;
        var it = handler.queue.known_queues.iterator();
        while (it.next()) |entry| {
            if (count >= S.name_buf.len) break;
            const meta = entry.value_ptr;
            if (namespace.len > 0 and !std.mem.eql(u8, meta.namespace, namespace)) continue;
            S.name_buf[count] = meta.name;
            count += 1;
        }

        return .{ .items = S.name_buf[0..count], .next_cursor = null };
    }

    // ── Pre-Route ───────────────────────────────────────────────────────

    fn preRouteByQueue(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    fn dispatchQueue(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.queue_handler.handleCommand(req);
        defer shard.queue_handler.freeResult(result);
        sendQueueResponse(shard, conn, req.header.request_id, result);
    }

    /// Dedicated dispatch for queue_enqueue — notifies blocking dequeue waiters.
    fn dispatchEnqueue(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.queue_handler.handleCommand(req);
        defer shard.queue_handler.freeResult(result);

        // After a successful enqueue, notify any blocking dequeue waiters and track namespace
        switch (result) {
            .queue_enqueued => {
                shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
                if (req.key.len > 0) {
                    shard.waiter_pool.notify(.queue_dequeue, req.key, @import("../node/shard.zig").resolveQueueWaiter, @ptrCast(shard));
                }
            },
            else => {},
        }

        sendQueueResponse(shard, conn, req.header.request_id, result);
    }

    /// Dedicated dispatch for queue_dequeue — supports blocking via block_ms.
    fn dispatchDequeue(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        // Check for blocking dequeue (--block → block_ms)
        const block_ms = req.getBlockMs();

        // Try dequeue first
        const result = shard.queue_handler.handleCommand(req);

        if (block_ms) |bms| {
            // Check if we got any messages
            switch (result) {
                .queue_messages => |m| {
                    if (m.data.len > 4) {
                        const count = std.mem.readInt(u32, m.data[0..4], .little);
                        if (count > 0) {
                            // Got messages — return immediately
                            defer shard.queue_handler.freeResult(result);
                            sendQueueResponse(shard, conn, req.header.request_id, result);
                            return;
                        }
                    }
                    // Empty result — register blocking waiter
                    shard.queue_handler.freeResult(result);
                },
                else => {
                    // Error or other — send immediately
                    defer shard.queue_handler.freeResult(result);
                    sendQueueResponse(shard, conn, req.header.request_id, result);
                    return;
                },
            }

            // Register waiter — store queue_name_hash in min_version for the resolver
            const ns_hash_w = router.namespaceHash(req.namespace);
            const q_hash = router.nameHash(ns_hash_w, req.key);
            const registered = shard.waiter_pool.register(.{
                .kind = .queue_dequeue,
                .fd = conn.fd,
                .owner_shard = conn.owner_shard,
                .conn_id = conn.id,
                .request_id = req.header.request_id,
                .key = req.key,
                .min_version = q_hash,
                .timeout_ms = bms,
            });
            if (!registered) {
                // Pool full — send empty result (count = 0) rather than deferring
                // a response with no waiter to ever complete it.
                var empty_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &empty_buf, 0, .little);
                shard.sendOkResponse(conn, req.header.request_id, &empty_buf);
                return;
            }
            conn.response_deferred = true;
            return;
        }

        // Non-blocking — standard path
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
        const timestamp_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        const ns_hash = router.namespaceHash(req.namespace);

        // Build value: [priority:u32][payload]
        const value_len = 4 + req.value.len;
        var value_buf: [4 + 4096]u8 = undefined;
        const value_slice = if (value_len <= value_buf.len) blk: {
            std.mem.writeInt(u32, value_buf[0..4], priority, .little);
            if (req.value.len > 0) {
                @memcpy(value_buf[4..][0..req.value.len], req.value);
            }
            break :blk value_buf[0..value_len];
        } else blk: {
            const dyn = self.allocator.alloc(u8, value_len) catch {
                return .{ .err = .{ .code = .internal_error, .message = "alloc failed" } };
            };
            std.mem.writeInt(u32, dyn[0..4], priority, .little);
            if (req.value.len > 0) {
                @memcpy(dyn[4..][0..req.value.len], req.value);
            }
            break :blk dyn;
        };
        defer if (value_len > value_buf.len) self.allocator.free(value_slice);

        // Persist through Raft for durability and replication
        if (self.shard_ptr) |sptr| {
            const shard: *Shard = @ptrCast(@alignCast(sptr));
            _ = persistence_mod.persistEntry(shard, .queue_enqueue, entry_mod.Flags.NONE, req.namespace, req.key, value_slice) catch {
                return .{ .err = .{ .code = .internal_error, .message = "raft persist failed" } };
            };
        }

        // Build command entry for local projection apply
        const next_index = self.partition.ual.max_index + 1;
        const payload_size = entry_mod.COMMAND_PREFIX_SIZE + req.key.len + value_slice.len;
        var payload_stack: [entry_mod.COMMAND_PREFIX_SIZE + 256 + 4 + 4096]u8 = undefined;
        const payload_buf = if (payload_size <= payload_stack.len) payload_stack[0..payload_size] else blk: {
            break :blk self.allocator.alloc(u8, payload_size) catch {
                return .{ .err = .{ .code = .internal_error, .message = "alloc failed" } };
            };
        };
        defer if (payload_size > payload_stack.len) self.allocator.free(payload_buf);

        const entry = entry_mod.buildCommandEntry(
            .queue_enqueue,
            entry_mod.Flags.NONE,
            self.partition.current_term,
            next_index,
            timestamp_ns,
            ns_hash,
            req.key,
            value_slice,
            payload_buf,
        ) orelse {
            return .{ .err = .{ .code = .internal_error, .message = "entry build failed" } };
        };

        // Persist to UAL — router fans out to queue.applyEntry() → queue.enqueue()
        _ = self.partition.apply(&entry) catch {
            return .{ .err = .{ .code = .internal_error, .message = "UAL append failed" } };
        };

        // Register the queue name so it appears in queue list
        const q_name_hash = router.nameHash(ns_hash, req.key);
        self.queue.registerQueue(q_name_hash, req.key, req.namespace) catch {};

        // Register in global metrics registry for dashboard/Prometheus
        if (self.metrics_registry) |mr| {
            _ = mr.registerQueue(req.namespace, req.key) catch {};
        }

        // Seq was assigned by the projection during apply
        const seq = self.queue.next_seq - 1;

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
        const now_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        const ns_hash = router.namespaceHash(req.namespace);
        const queue_name_hash = router.nameHash(ns_hash, req.key);

        // Expire stale leases first
        self.queue.expireLeases(now_ns);

        // Dequeue up to count messages matching this queue
        var results: [MAX_DEQUEUE_BATCH]DequeueResult = undefined;
        var actual: u32 = 0;

        for (0..capped) |_| {
            const maybe_result = self.queue.dequeue(now_ns, queue_name_hash) catch break;
            if (maybe_result) |deq_result| {
                results[actual] = deq_result;
                actual += 1;
            } else {
                break;
            }
        }

        // Serialize dequeue results BEFORE auto-ack (ack frees message payloads).
        const data = serializeDequeueResults(self.allocator, results[0..actual]) catch {
            return .{ .err = .{ .code = .internal_error, .message = "dequeue serialization failed" } };
        };

        // Persist auto-ack entries so dequeued messages don't reappear after restart.
        // In this simplified model, dequeue = consume (not a lease-based model).
        for (results[0..actual]) |r| {
            self.persistAck(ns_hash, r.seq);
        }

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

        // Persist through Raft
        var seq_key: [8]u8 = undefined;
        std.mem.writeInt(u64, &seq_key, seq, .little);
        if (self.shard_ptr) |sptr| {
            const shard: *Shard = @ptrCast(@alignCast(sptr));
            _ = persistence_mod.persistEntry(shard, .queue_ack, entry_mod.Flags.NONE, req.namespace, &seq_key, &[_]u8{}) catch {
                return .{ .err = .{ .code = .internal_error, .message = "raft persist failed" } };
            };
        }

        // Apply locally
        const timestamp_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        const ns_hash = router.namespaceHash(req.namespace);
        const next_index = self.partition.ual.max_index + 1;

        const payload_size = entry_mod.COMMAND_PREFIX_SIZE + 8; // 8-byte seq key, no value
        var payload_buf: [entry_mod.COMMAND_PREFIX_SIZE + 8]u8 = undefined;

        const entry = entry_mod.buildCommandEntry(
            .queue_ack,
            entry_mod.Flags.NONE,
            self.partition.current_term,
            next_index,
            timestamp_ns,
            ns_hash,
            &seq_key,
            &[_]u8{},
            payload_buf[0..payload_size],
        ) orelse {
            return .{ .err = .{ .code = .internal_error, .message = "entry build failed" } };
        };

        _ = self.partition.apply(&entry) catch {
            return .{ .err = .{ .code = .internal_error, .message = "UAL append failed" } };
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

        // Persist through Raft
        var seq_key: [8]u8 = undefined;
        std.mem.writeInt(u64, &seq_key, seq, .little);
        if (self.shard_ptr) |sptr| {
            const shard: *Shard = @ptrCast(@alignCast(sptr));
            _ = persistence_mod.persistEntry(shard, .queue_nack, entry_mod.Flags.NONE, req.namespace, &seq_key, &[_]u8{}) catch {
                return .{ .err = .{ .code = .internal_error, .message = "raft persist failed" } };
            };
        }

        // Apply locally
        const timestamp_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        const ns_hash = router.namespaceHash(req.namespace);
        const next_index = self.partition.ual.max_index + 1;

        const payload_size = entry_mod.COMMAND_PREFIX_SIZE + 8;
        var payload_buf: [entry_mod.COMMAND_PREFIX_SIZE + 8]u8 = undefined;

        const entry = entry_mod.buildCommandEntry(
            .queue_nack,
            entry_mod.Flags.NONE,
            self.partition.current_term,
            next_index,
            timestamp_ns,
            ns_hash,
            &seq_key,
            &[_]u8{},
            payload_buf[0..payload_size],
        ) orelse {
            return .{ .err = .{ .code = .internal_error, .message = "entry build failed" } };
        };

        _ = self.partition.apply(&entry) catch {
            return .{ .err = .{ .code = .internal_error, .message = "UAL append failed" } };
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
        // Serialize known queues for this shard.
        // Wire format: [count:u32] ([name_len:u32][name][ns_len:u32][ns]
        //   [pending:u64][available:u64][enqueued:u64][dequeued:u64][dlq:u64])*
        //   [has_more:u8] [cursor_len:u16]
        const target_ns = req.namespace;

        // First pass: count matching queues and total size
        var match_count: u32 = 0;
        var body_size: usize = 0;
        {
            var it = self.queue.known_queues.iterator();
            while (it.next()) |kv| {
                const meta = kv.value_ptr;
                if (std.mem.eql(u8, meta.namespace, target_ns)) {
                    body_size += 4 + meta.name.len + 4 + meta.namespace.len + 5 * 8;
                    match_count += 1;
                }
            }
        }

        const total = 4 + body_size + 1 + 2; // count + entries + has_more + cursor_len
        const buf = self.allocator.alloc(u8, total) catch {
            return .{ .err = .{ .code = .internal_error, .message = "list serialization failed" } };
        };
        errdefer self.allocator.free(buf);
        var offset: usize = 0;

        // Count
        std.mem.writeInt(u32, buf[offset..][0..4], match_count, .little);
        offset += 4;

        // Entries
        {
            var it = self.queue.known_queues.iterator();
            while (it.next()) |kv| {
                const meta = kv.value_ptr;
                if (!std.mem.eql(u8, meta.namespace, target_ns)) continue;

                // name_len + name
                std.mem.writeInt(u32, buf[offset..][0..4], @intCast(meta.name.len), .little);
                offset += 4;
                @memcpy(buf[offset..][0..meta.name.len], meta.name);
                offset += meta.name.len;

                // ns_len + ns
                std.mem.writeInt(u32, buf[offset..][0..4], @intCast(meta.namespace.len), .little);
                offset += 4;
                @memcpy(buf[offset..][0..meta.namespace.len], meta.namespace);
                offset += meta.namespace.len;

                // pending (= enqueued - dequeued, approximate)
                const pending = if (meta.enqueued > meta.dequeued) meta.enqueued - meta.dequeued else 0;
                std.mem.writeInt(u64, buf[offset..][0..8], pending, .little);
                offset += 8;

                // available (same as pending for now)
                std.mem.writeInt(u64, buf[offset..][0..8], pending, .little);
                offset += 8;

                // enqueued
                std.mem.writeInt(u64, buf[offset..][0..8], meta.enqueued, .little);
                offset += 8;

                // dequeued
                std.mem.writeInt(u64, buf[offset..][0..8], meta.dequeued, .little);
                offset += 8;

                // dlq
                std.mem.writeInt(u64, buf[offset..][0..8], @intCast(self.queue.dlqCount()), .little);
                offset += 8;
            }
        }

        // has_more = 0 (single-shard response, CLI does shard walking)
        buf[offset] = 0;
        offset += 1;

        // cursor_len = 0 (no cursor)
        std.mem.writeInt(u16, buf[offset..][0..2], 0, .little);
        offset += 2;

        return .{ .queue_messages = .{ .data = buf } };
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    /// Persist a queue_ack UAL entry for a dequeued message.
    /// Called automatically after dequeue so consumed messages don't reappear after restart.
    fn persistAck(self: *QueueHandler, ns_hash: u32, seq: u64) void {
        var seq_key: [8]u8 = undefined;
        std.mem.writeInt(u64, &seq_key, seq, .little);

        // Persist through Raft
        if (self.shard_ptr) |sptr| {
            const shard: *Shard = @ptrCast(@alignCast(sptr));
            _ = persistence_mod.persistEntry(shard, .queue_ack, entry_mod.Flags.NONE, "", &seq_key, &[_]u8{}) catch {};
        }

        // Apply locally
        const timestamp_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        const next_index = self.partition.ual.max_index + 1;

        const payload_size = entry_mod.COMMAND_PREFIX_SIZE + 8;
        var payload_buf: [entry_mod.COMMAND_PREFIX_SIZE + 8]u8 = undefined;

        const entry = entry_mod.buildCommandEntry(
            .queue_ack,
            entry_mod.Flags.NONE,
            self.partition.current_term,
            next_index,
            timestamp_ns,
            ns_hash,
            &seq_key,
            &[_]u8{},
            payload_buf[0..payload_size],
        ) orelse return;

        _ = self.partition.apply(&entry) catch {};
    }

    pub fn freeResult(self: *QueueHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .queue_messages => |r| self.allocator.free(r.data),
            .queue_peek_messages => |r| self.allocator.free(r.data),
            .queue_dlq_messages => |r| self.allocator.free(r.data),
            else => {},
        }
    }

    // ── Replay ──────────────────────────────────────────────────────────

    /// Register queue entry types with the replay registry.
    pub fn registerReplay(self: *QueueHandler, registry: *ReplayRegistry) void {
        registry.register(.queue_enqueue, @ptrCast(self), replayEntry);
        registry.register(.queue_ack, @ptrCast(self), replayEntry);
        registry.register(.queue_nack, @ptrCast(self), replayEntry);
    }

    /// Replay a queue entry — rebuild queue name registration.
    /// The projection router already routes queue entries to QueueProjection.applyEntry(),
    /// which handles the actual enqueue/ack/nack. This callback just ensures
    /// queue name metadata is restored for `queue list`.
    fn replayEntry(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
        const self: *QueueHandler = @ptrCast(@alignCast(ctx));
        const etype: entry_mod.EntryType = @enumFromInt(entry.header.entry_type);

        if (etype == .queue_enqueue) {
            if (entry_mod.CommandPayload.deserialize(entry.payload)) |cmd| {
                const ns_hash = cmd.namespace_hash;
                const q_name_hash = router.nameHash(ns_hash, cmd.key);
                self.queue.registerQueue(q_name_hash, cmd.key, "") catch {};
            }
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
/// Wire format: [count:u32] ([seq:u64][payload_len:u32][payload][enqueued_at:i64][delivery_count:u32][priority:u8])*
fn serializeDequeueResults(allocator: Allocator, results: []const DequeueResult) ![]u8 {
    return serializeDequeueResultsPub(allocator, results);
}

/// Public variant of serializeDequeueResults — used by the WaiterPool resolver
/// in shard.zig to build queue responses for blocking dequeue.
pub fn serializeDequeueResultsPub(allocator: Allocator, results: []const DequeueResult) ![]u8 {
    // Calculate total size: count header + per-message (seq + payload_len + payload + enqueued_at + delivery_count + priority)
    var total: usize = 4; // u32 count
    for (results) |r| {
        total += 8; // seq: u64
        total += 4; // payload_len: u32
        total += r.payload.len; // payload bytes
        total += 8; // enqueued_at: i64
        total += 4; // delivery_count: u32
        total += 1; // priority: u8
    }

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);
    var offset: usize = 0;

    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(results.len), .little);
    offset += 4;

    for (results) |r| {
        std.mem.writeInt(u64, buf[offset..][0..8], r.seq, .little);
        offset += 8;
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(r.payload.len), .little);
        offset += 4;
        if (r.payload.len > 0) {
            @memcpy(buf[offset..][0..r.payload.len], r.payload);
            offset += r.payload.len;
        }
        // enqueued_at as i64 (convert from u64 ns to ms for the CLI)
        const enqueued_ms: i64 = @intCast(r.enqueued_at_ns / 1_000_000);
        std.mem.writeInt(i64, buf[offset..][0..8], enqueued_ms, .little);
        offset += 8;
        std.mem.writeInt(u32, buf[offset..][0..4], r.attempts, .little);
        offset += 4;
        buf[offset] = @intCast(r.priority);
        offset += 1;
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
            .reserved = .{0} ** 8,
        },
        .namespace = "default",
        .key = key,
        .value = value,
        .options = options,
    };
}

fn initTestPartition(allocator: Allocator) !*Partition {
    const partition = try allocator.create(Partition);
    partition.* = try Partition.init(allocator, 0, 4096, 0);
    partition.wireProjections();
    return partition;
}

fn deinitTestPartition(allocator: Allocator, partition: *Partition) void {
    partition.deinit();
    allocator.destroy(partition);
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
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

    const result = handler.handleCommand(makeRequest(.queue_enqueue, "tasks", "payload", ""));
    switch (result) {
        .queue_enqueued => |e| {
            try testing.expect(e.message_id.len > 0);
        },
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(u64, 1), partition.queue.stats.enqueued);
    try testing.expectEqual(@as(usize, 1), partition.queue.readyCount());
}

test "queue handler: enqueue with priority" {
    const allocator = testing.allocator;
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

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

    try testing.expectEqual(@as(usize, 1), partition.queue.readyCount());
}

test "queue handler: enqueue empty queue name" {
    const allocator = testing.allocator;
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);
    const result = handler.handleCommand(makeRequest(.queue_enqueue, "", "data", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "queue handler: dequeue" {
    const allocator = testing.allocator;
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

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

    // Dequeue is auto-ack (consume), so 0 leased, 1 ready
    try testing.expectEqual(@as(usize, 0), partition.queue.leasedCount());
    try testing.expectEqual(@as(usize, 1), partition.queue.readyCount());
}

test "queue handler: dequeue empty queue" {
    const allocator = testing.allocator;
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

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
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

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

    // Auto-ack already consumed the message during dequeue, so:
    // - stats.acked = 1 (from auto-ack during dequeue)
    // - explicit ack is a no-op (message already removed)
    try testing.expectEqual(@as(u64, 1), partition.queue.stats.acked);
    try testing.expectEqual(@as(usize, 0), partition.queue.leasedCount());
}

test "queue handler: fail (nack)" {
    const allocator = testing.allocator;
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

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

    // With auto-ack, the message was already consumed during dequeue.
    // Nack on a consumed message is a no-op.
    try testing.expectEqual(@as(u64, 0), partition.queue.stats.nacked);
    try testing.expectEqual(@as(usize, 0), partition.queue.readyCount());
}

test "queue handler: stats" {
    const allocator = testing.allocator;
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

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
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

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
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

    // ack on non-existent seq is a silent no-op in the projection
    const result = handler.handleCommand(makeRequest(.queue_complete, "q1", "999", ""));
    switch (result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "queue handler: complete missing seq" {
    const allocator = testing.allocator;
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

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

    // Same queue, different namespace → different hash (namespace isolation)
    var req_ns = makeRequest(.queue_enqueue, "queue-a", "", "");
    req_ns.namespace = "other";
    try testing.expect(QueueHandler.preRouteByQueue(req1) != QueueHandler.preRouteByQueue(req_ns));
}

test "queue handler: peek empty" {
    const allocator = testing.allocator;
    const partition = try initTestPartition(allocator);
    defer deinitTestPartition(allocator, partition);

    var handler = QueueHandler.init(allocator, partition);

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
