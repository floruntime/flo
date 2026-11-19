//! Stream Handler — registers stream and consumer-group opcodes with Dispatcher.
//!
//! Read operations (read, info, group_pending) find a stream's appends in
//! the streams machine's families (`stream/machine.zig`, read through
//! `StreamProjection`), then read payloads directly from the UAL
//! (zero-copy when the entry is contiguous in the hot ring).
//!
//! Write operations (create, alter, append, trim, delete) persist an entry
//! through Raft; the streams machine applies it, and the client is
//! answered from what it recorded.
//!
//! Stream data entries live in the UAL and are read directly: the UAL
//! entry is the stream record, and `stream.record` says where it is.
//!
//! ## Opcode Ranges
//!
//!   Streams:         0x130–0x140 (append, read, trim, info, subscribe, list, create, alter, delete)
//!   Consumer groups: 0x150–0x15E, served by `group_handler.zig`
//!
//! ## Data Model
//!
//! Each append's `stream.record` maps its first StreamID to its ual_index;
//! the message payloads live in the UAL there.
//! A consumer group leases each record it delivers until acked; its state
//! is the group machine's, read from the stream at apply.

const std = @import("std");
const stdx = @import("stdx");
const log = @import("stdx").log;
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
const table_run = @import("../node/table_run.zig");
const stream_ops = @import("../protocol/stream_ops.zig");
const schema = @import("../protocol/schema.zig");
const group_handler = @import("group_handler.zig");
const group_machine = @import("group_machine.zig");
const stream_machine = @import("machine.zig");
const contract_machine = @import("../contract/machine.zig");
const ReadView = @import("../contract/view.zig").ReadView;
const group_entries = @import("../protocol/group_entries.zig");
const GroupListResponse = table_run.op("stream.group.list").response;
const GroupMembersResponse = table_run.op("stream.group.members").response;

const CommandResult = result_mod.CommandResult;
const disk_reads = @import("disk_reads.zig");
const StreamProjection = stream_mod.StreamProjection;
const StreamKey = stream_mod.StreamKey;
const StreamID = stream_mod.StreamID;
const time_units = @import("../util/time_units.zig");

pub const TrimBound = stream_mod.TrimBound;
const StreamRecord = stream_mod.StreamRecord;
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
const Result = persistence_mod.Result;
const ReplayRegistry = persistence_mod.ReplayRegistry;
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;
const TieredLogMetrics = @import("../metrics/registry.zig").TieredLogMetrics;

// ═══════════════════════════════════════════════════════════════════════════════
// StreamHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const StreamHandler = struct {
    stream: *StreamProjection,
    partition: *Partition,
    allocator: Allocator,

    /// Opaque pointer to owning Shard (avoids circular import).
    /// Set after init by Shard.init. Required to propose Raft entries.
    shard_ptr: ?*anyopaque,

    /// Global metrics registry (optional, set by runtime when dashboard is enabled).
    metrics_registry: ?*MetricsRegistry,

    /// Tier-hit counters for this shard's log, resolved once at wire-up so the
    /// read path does not do a registry lookup per record.
    tiered_metrics: ?*TieredLogMetrics = null,

    /// Payloads past the hot ring and the warm store, read back from the
    /// shard's sealed segments. Null in tests that build no durable log.
    disk: ?*disk_reads.DiskReads = null,
    /// Set by a read that met a payload being read from disk (`pending`) or
    /// that can't be read right now (`blocked`): its answer is held, not
    /// sent, and the read is made again once the disk read is back. Reset
    /// by `beginRead`.
    held: enum { no, pending, blocked } = .no,
    /// The stream and window the read in progress asks the disk for, so one
    /// read back serves the whole window.
    disk_window: disk_reads.Window = .{ .stream = @splat(0), .from = 0, .limit = 0 },
    /// How many of the records the last serialize was given it answered: up
    /// to the first one being read back from disk.
    served_records: usize = 0,

    /// Appends a read had to skip: not in hot, warm or disk storage, or not
    /// a well-formed batch. Each is counted and logged once.
    skipped_appends: u64 = 0,
    /// The highest log index counted in `skipped_appends`: reads move
    /// forward, so a re-read or a waiter's next wake doesn't count it again.
    highest_skipped_ual: u64 = 0,
    /// Its `part`: an inbound holds several appends at one index.
    highest_skipped_part: u8 = 0,
    /// Backs a refusal message that names request values; sent before the
    /// next command runs.
    err_buf: [128]u8 = undefined,
    /// A group list or members answer, encoded after the handler returns.
    list_buf: ?[]u8 = null,
    /// A group operator command's answer, built as it is answered.
    op_buf: ?[]u8 = null,
    /// Consumer groups' reads in flight and expire duty (`group_handler.zig`).
    groups: group_handler.Groups,

    /// Maximum number of messages in a single read response.
    pub const MAX_READ_BATCH: usize = proto.MAX_STREAM_READ_RECORDS;

    comptime {
        // One append, the most a read can't split, always fits an answer.
        std.debug.assert(4 + 2 * persistence_mod.MAX_PERSIST_PAYLOAD + 28 * @as(usize, stream_mod.MAX_BATCH_RECORDS) <= proto.MAX_ANSWER_BYTES);
    }
    const DEFAULT_READ_BATCH: usize = 100;

    pub fn init(allocator: Allocator, partition: *Partition) StreamHandler {
        return .{
            .stream = &partition.stream,
            .partition = partition,
            .allocator = allocator,
            .shard_ptr = null,
            .metrics_registry = null,
            .groups = group_handler.Groups.init(allocator),
        };
    }

    pub fn deinit(self: *StreamHandler) void {
        if (self.list_buf) |b| self.allocator.free(b);
        if (self.op_buf) |b| self.allocator.free(b);
        self.groups.deinit();
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
        dispatcher.registerWithRoute(.stream_delete, dispatchStream, preRouteByStream);

        // Consumer groups, on the group machine (`group_handler.zig`).
        dispatcher.registerWithRoute(.stream_group_create, group_handler.dispatchCreate, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_delete, group_handler.dispatchDelete, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_read, group_handler.dispatchRead, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_ack, group_handler.dispatchAck, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_nack, group_handler.dispatchNack, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_touch, group_handler.dispatchTouch, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_info, group_handler.dispatchInfo, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_pending, group_handler.dispatchPending, preRouteByStream);
        inline for (group_handler.REMOVED) |op|
            dispatcher.registerWithRoute(op, group_handler.dispatchRemoved, preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_list, table_run.handler("stream.group.list", StreamHandler, runGroupList), preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_members, table_run.handler("stream.group.members", StreamHandler, runGroupMembers), preRouteByStream);
        const Op = group_handler.Operator;
        dispatcher.registerWithRoute(.stream_group_steal, table_run.handler("stream.group.steal", Op, Op.runSteal), preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_dlq_list, table_run.handler("stream.group.dlq_list", Op, Op.runDlqList), preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_dlq_requeue, table_run.handler("stream.group.dlq_requeue", Op, Op.runRequeue), preRouteByStream);
        dispatcher.registerWithRoute(.stream_group_dlq_delete, table_run.handler("stream.group.dlq_delete", Op, Op.runDelete), preRouteByStream);
    }

    // ── Group list and members ──────────────────────────────────────────

    pub const Error = error{ OutOfMemory, NameTooLong };

    pub fn refusal(err: Error) table_run.Refused {
        return switch (err) {
            error.OutOfMemory => .{ .status = .overloaded, .refusal = proto.notRun("overloaded: no memory to list this; retry") },
            error.NameTooLong => .{ .status = .bad_request, .refusal = proto.notRun("bad request: stream or group name too long") },
        };
    }

    pub fn opAnswerBuf(self: *StreamHandler) error{OutOfMemory}![]u8 {
        if (self.op_buf) |b| return b;
        const b = try self.allocator.alloc(u8, @max(stream_ops.DeadPage.byte_bound.?, schema.List(stream_ops.Refused, @import("../protocol/group_entries.zig").MAX_BATCH).max_size));
        self.op_buf = b;
        return b;
    }

    fn answerBuf(self: *StreamHandler) Error![]u8 {
        if (self.list_buf) |b| return b;
        const b = try self.allocator.alloc(u8, @max(stream_ops.Groups.byte_bound.?, stream_ops.Members.byte_bound.?));
        self.list_buf = b;
        return b;
    }

    /// The stream's groups, from the group machine's metas: a stream's
    /// groups' scopes lead with its name hash. A meta of another stream whose
    /// name hashes the same is passed over.
    fn runGroupList(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(GroupListResponse) {
        const self = shard.stream_handler;
        if (req.key.len > group_entries.StreamName.max_size) return error.NameTooLong;
        const sk = group_machine.streamOf(req.ns_id, req.key);
        const hb = sk[4..12];
        var kb: [12]u8 = undefined;
        // By name, so a list that doesn't fit always shows the same first ones.
        const Named = struct { name: []const u8, m: group_machine.Meta.Value, scope: group_machine.Scope };
        var found: std.ArrayList(Named) = .empty;
        defer found.deinit(self.allocator);
        const read: @import("../contract/view.zig").ReadView = .{ .store = &self.partition.store };
        var it = read.range(group_machine.Metas, group_machine.storeKey(&kb, req.ns_id, hb));
        while (it.next() catch null) |item| {
            if (!std.mem.eql(u8, item.value.v.stream, req.key)) continue;
            // A gone group's records are still being cleaned: it isn't listed.
            if (!group_machine.live(item.value.v, group_handler.streamRecOf(shard, sk))) continue;
            try found.append(self.allocator, .{ .name = item.value.v.group, .m = item.value.v, .scope = item.key[4..][0..group_machine.SCOPE_LEN].* });
        }
        std.mem.sort(Named, found.items, {}, struct {
            fn lessThan(_: void, a: Named, b: Named) bool {
                return std.mem.order(u8, a.name, b.name) == .lt;
            }
        }.lessThan);
        var out = stream_ops.Groups.Builder.init(try self.answerBuf());
        var more = false;
        for (found.items) |f| {
            var members: [stream_ops.MAX_MEMBERS]Holder = undefined;
            const n = holdersOf(read, req.ns_id, f.scope, &members);
            const added = out.add(.{
                .name = f.name,
                .members = @intCast(n),
                .pending = f.m.ready + f.m.leased,
                .created_at_ms = @intCast(f.m.created_ns / std.time.ns_per_ms),
                .ack_timeout_ms = @intCast(f.m.lease_ms),
                .max_deliver = @intCast(@min(f.m.max_attempts, 255)),
                .last_delivered_ms = f.m.cursor_ms,
                .last_delivered_seq = f.m.cursor_seq,
            }) catch false;
            if (!added) {
                more = true;
                break;
            }
        }
        return .{ .answer = .{ .groups = out.value(), .more = more } };
    }

    /// A holder of a group's records out now: how many it holds, since
    /// when, and when it last took one.
    const Holder = struct { id: []const u8, held: u32, since_ns: u64, last_ns: u64 };

    fn holdersOf(read: @import("../contract/view.zig").ReadView, ns: router.NamespaceId, scope: group_machine.Scope, out: []Holder) usize {
        var n: usize = 0;
        var kb: [4 + group_machine.SCOPE_LEN]u8 = undefined;
        var it = read.range(group_machine.Items, group_machine.storeKey(&kb, ns, &scope));
        while (it.next() catch null) |item| {
            const v = item.value.v;
            if (v.status != .leased) continue;
            for (out[0..n]) |*h| {
                if (!std.mem.eql(u8, h.id, v.holder)) continue;
                h.held += 1;
                h.since_ns = @min(h.since_ns, v.delivered_ns);
                h.last_ns = @max(h.last_ns, v.delivered_ns);
                break;
            } else {
                if (n == out.len) continue;
                out[n] = .{ .id = v.holder, .held = 1, .since_ns = v.delivered_ns, .last_ns = v.delivered_ns };
                n += 1;
            }
        }
        return n;
    }

    /// A group's members: the consumers holding its records now. Membership
    /// isn't logged; a consumer with nothing out isn't one.
    fn runGroupMembers(shard: *Shard, _: *Connection, req: Request, group: []const u8) Error!table_run.Outcome(GroupMembersResponse) {
        const self = shard.stream_handler;
        if (req.key.len > group_entries.StreamName.max_size or group.len > group_entries.MAX_GROUP_NAME) return error.NameTooLong;
        _ = group_handler.metaOf(shard, req.ns_id, req.key, group) orelse
            return .{ .refused = .{ .status = .not_found, .refusal = .{ .reason = .not_found, .ran = .no, .message = "not found: no such consumer group" } } };
        const read: @import("../contract/view.zig").ReadView = .{ .store = &self.partition.store };
        var holders: [stream_ops.MAX_MEMBERS]Holder = undefined;
        const n = holdersOf(read, req.ns_id, group_machine.scopeOf(req.ns_id, req.key, group), &holders);
        // By id, so a list that doesn't fit always shows the same first ones.
        std.mem.sort(Holder, holders[0..n], {}, struct {
            fn lessThan(_: void, a: Holder, b: Holder) bool {
                return std.mem.order(u8, a.id, b.id) == .lt;
            }
        }.lessThan);
        var out = stream_ops.Members.Builder.init(try self.answerBuf());
        var more = false;
        for (holders[0..n]) |h| {
            const added = out.add(.{
                .id = h.id,
                .state = .active,
                .pending = h.held,
                .joined_at_ms = @intCast(h.since_ns / std.time.ns_per_ms),
                .last_active_ms = @intCast(h.last_ns / std.time.ns_per_ms),
            }) catch false;
            if (!added) {
                more = true;
                break;
            }
        }
        return .{ .answer = .{ .members = out.value(), .more = more } };
    }

    // ── Pre-Route Hooks ─────────────────────────────────────────────────

    /// Route by stream name — single-partition operations.
    /// Routed by the namespace's id and the stream's name (`routeNsKey`).
    pub fn preRouteByStream(req: Request) ?u64 {
        if (req.key.len == 0) return null;
        return router.routeNsKey(req.ns_id, req.key);
    }

    // ── Shard Walker: Local Scan ────────────────────────────────────────

    /// ShardWalker LocalScanFn for stream_list — the names of one
    /// namespace's streams on this shard (`stream.meta`).
    pub fn localScanStreams(
        ctx: *anyopaque,
        ns: dispatcher_mod.WalkNamespace,
        _: []const u8,
        _: ?[]const u8,
        _: u32,
    ) dispatcher_mod.NameWalker.ScanResult {
        const self: *StreamHandler = @ptrCast(@alignCast(ctx));
        const prefix = ns.id.bytes();
        var n: usize = 0;
        var counting = self.readView().range(stream_machine.Metas, &prefix);
        // A tombstone (a deleted stream its groups still count) is no stream.
        while (counting.next() catch null) |item| {
            if (item.value.v.exists) n += 1;
        }
        const name_buf = dispatcher_mod.scanScratch(n);
        var count: usize = 0;
        var it = self.readView().range(stream_machine.Metas, &prefix);
        while (it.next() catch null) |item| {
            if (!item.value.v.exists) continue;
            if (count == name_buf.len) break;
            name_buf[count] = item.value.v.name;
            count += 1;
        }
        return .{ .items = name_buf[0..count], .next_cursor = null };
    }

    /// `name`'s entry as a stream list shows it: its name, partition count
    /// and retention, 0 for no limit; nothing for a stream that doesn't
    /// exist.
    pub fn writeListEntry(self: *const StreamHandler, ns: router.NamespaceId, name: []const u8, w: *std.Io.Writer) std.Io.Writer.Error!void {
        const m = stream_machine.metaOf(self.readView(), ns, name) orelse return;
        try w.writeInt(u32, @intCast(name.len), .little);
        try w.writeAll(name);
        try w.writeInt(u32, m.partition_count, .little);
        try w.writeInt(u64, m.retention_age_s, .little);
        try w.writeInt(u64, m.retention_count, .little);
    }

    // ── Dispatch Wrappers ───────────────────────────────────────────────

    fn dispatchStream(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.stream_handler.handleCommand(req);
        defer shard.stream_handler.freeResult(cmd_result);
        if (cmd_result == .parked) return parkStream(shard, conn, req, cmd_result.parked);

        sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
    }

    /// Park a stream write on its entry, answered from what its applier
    /// recorded. An append counts against its namespace here, where it is
    /// accepted.
    fn parkStream(shard: *Shard, conn: *Connection, req: Request, proposed: persistence_mod.ProposeResult) void {
        const op: proto.OpCode = @fromBackingInt(@intCast(req.header.op_code));
        if (op == .stream_append or op == .stream_create) shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        shard.park(conn, req, proposed, switch (op) {
            .stream_append => answerAppend,
            .stream_trim => answerTrim,
            else => answerOk,
        });
    }

    fn answerAppend(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        sendStreamResponse(shard, conn, request_id, appendedFrom(result));
    }

    fn answerTrim(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        const deleted = r.int(u64) orelse 0;
        sendStreamResponse(shard, conn, request_id, .{ .stream_trimmed = .{ .deleted_count = deleted, .first_seq = r.int(u64) orelse 0 } });
    }

    fn answerOk(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        sendStreamResponse(shard, conn, request_id, .ok);
    }

    /// An append's answer: the id its applier gave it.
    fn appendedFrom(result: *const Result) CommandResult {
        var r = result.reader();
        const seq = r.int(u64) orelse 0;
        return appendOk(.{ .sequence = seq, .timestamp_ms = r.int(u64) orelse 0 });
    }

    /// Dedicated dispatch for stream_append; parks until the append commits.
    fn dispatchAppend(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.stream_handler.handleCommand(req);
        defer shard.stream_handler.freeResult(cmd_result);
        if (cmd_result == .parked) return parkStream(shard, conn, req, cmd_result.parked);
        switch (cmd_result) {
            .stream_append_ok => shard.namespace_handler.markNamespaceHasData(req.namespace, shard),
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
                .pending => return holdForDisk(shard, conn, req, bms),
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
                // A stream not there yet: a blocking read waits for its
                // first append, as it waits for any record.
                .err => |e| if (e.code == .not_found) shard.stream_handler.freeResult(cmd_result) else {
                    defer shard.stream_handler.freeResult(cmd_result);
                    sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
                    return;
                },
                else => {
                    // Error or other result — send immediately
                    defer shard.stream_handler.freeResult(cmd_result);
                    sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
                    return;
                },
            }

            // Park on the window just read (`Shard.resolveStreamWaiter`).
            const registered = shard.waiter_pool.register(.{
                .kind = .stream_read,
                .reply_to = conn.replyTo(),
                .request_id = req.header.request_id,
                .key = req.key,
                .stream = shard.stream_handler.readWindow(req),
                .timeout_ms = bms,
            });
            if (!registered) return shard.refuseWaitersFull(conn, req.header.request_id);
            conn.response_deferred = true;
            return;
        }

        // Non-blocking read — standard path
        const cmd_result = shard.stream_handler.handleCommand(req);
        if (cmd_result == .pending) return holdForDisk(shard, conn, req, DISK_READ_WAIT_MS);
        defer shard.stream_handler.freeResult(cmd_result);
        // No such stream here, on a follower: the leader answers whether
        // there is one, or this answers stale.
        const missing = cmd_result == .err and cmd_result.err.code == .not_found;
        if (missing and shard.missToLeader(conn, req)) return;
        const was_stale = proto.answering_stale;
        defer proto.answering_stale = was_stale;
        if (missing and shard.staleMiss(req.header.op_code)) proto.answering_stale = true;
        sendStreamResponse(shard, conn, req.header.request_id, cmd_result);
    }

    /// The longest a read waits on payloads being read back from disk. A
    /// local read takes milliseconds; the bound is for a failing disk.
    pub const DISK_READ_WAIT_MS: u32 = 2_000;
    /// Reads held on disk reads at once, per shard.
    pub const MAX_DISK_HELD: u16 = 64;

    /// A read that met payloads being read back from disk: held, and made
    /// again once they are (`Shard.resolveStreamWaiter`). One that can't be
    /// read right now is refused retryable at once: nothing is in flight to
    /// wait on.
    fn holdForDisk(shard: *Shard, conn: *Connection, req: Request, wait_ms: u32) void {
        // Past its own cap, a cold read is refused rather than take waiter
        // slots KV watches and dequeues share.
        if (shard.stream_handler.held != .pending) {
            return shard.sendRefusal(conn, req.header.request_id, .unavailable, proto.notRun(shard.stream_handler.diskRefusal()));
        }
        if (shard.waiter_pool.countByKind(.stream_disk_read) >= MAX_DISK_HELD) {
            return shard.sendRefusal(conn, req.header.request_id, .unavailable, proto.notRun(shard_mod.DISK_READS_FULL));
        }
        const registered = shard.waiter_pool.register(.{
            .kind = .stream_disk_read,
            .reply_to = conn.replyTo(),
            .request_id = req.header.request_id,
            .key = req.key,
            .stream = shard.stream_handler.readWindow(req),
            .timeout_ms = @min(wait_ms, DISK_READ_WAIT_MS),
        });
        if (!registered) return shard.refuseWaitersFull(conn, req.header.request_id);
        conn.response_deferred = true;
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    /// Why a request's stream name cannot be used, or null: none, one
    /// holding a NUL, or one too long for a key. Checked here for every
    /// request but list, before any key is built.
    fn nameRefusal(op: OpCode, req: Request) ?[]const u8 {
        if (op == .stream_list) return null; // key is a filter, never a name
        if (std.mem.indexOfScalar(u8, req.key, 0) != null) return "stream name must not contain NUL";
        if (req.key.len == 0) return "stream name is required";
        if (req.key.len > ns_keys.MAX_QUALIFIED_KEY) return "stream name too long";
        return null;
    }

    const NAME_TOO_LONG: CommandResult = .{ .err = .{ .code = .invalid_request, .message = "stream name too long" } };

    /// An append through this handler's own entry point, for tests in other
    /// files; it fails to compile outside tests, so nothing reaches the
    /// handler around `run()`.
    pub fn appendForTest(self: *StreamHandler, namespace: []const u8, name: []const u8, batch: []const u8) CommandResult {
        if (!@import("builtin").is_test) @compileError("appendForTest is for tests");
        return self.handleCommand(.{ .header = proto.RequestHeader.init(.stream_append, 0), .namespace = namespace, .ns_id = self.namespaceId(namespace), .key = name, .value = batch, .options = "" });
    }

    /// Dispatch a stream command to the appropriate handler.
    fn handleCommand(self: *StreamHandler, req: Request) CommandResult {
        self.beginRead();
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        if (nameRefusal(op, req)) |why| return .{ .err = .{ .code = .invalid_request, .message = why } };
        return switch (op) {
            // Stream operations
            .stream_append => self.handleAppend(req),
            .stream_read => self.handleRead(req),
            .stream_trim => self.handleTrim(req),
            .stream_info => self.handleInfo(req),
            .stream_create => self.handleCreate(req),
            .stream_alter => self.handleAlter(req),
            .stream_delete => self.handleDelete(req),

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
                const pc = stream_machine.partitionCount(self.readView(), req.ns_id, req.key);
                partition_index = @intCast(std.hash.Wyhash.hash(0, pk_bytes) % pc);
            }
        }

        const payload_value = req.value;
        _ = stream_mod.BatchIterator.init(payload_value) catch |err| return .{ .err = .{
            .code = .invalid_request,
            .message = switch (err) {
                error.TooManyRecords => std.fmt.bufPrint(&self.err_buf, "stream append: a batch of {d} records is over the limit of {d}", .{ stream_mod.BatchIterator.claimedCount(payload_value), stream_mod.MAX_BATCH_RECORDS }) catch "stream append: batch over the record limit",
                error.Malformed => "stream append: malformed batch",
            },
        } };
        // The append goes through Raft and is applied from the committed log
        // by the shard's one applier, so it replicates and survives restart;
        // the client is parked until then and answered with the StreamID its
        // applier recorded. Without a shard (unit tests) the same applier runs
        // on a locally built entry.
        if (self.shard_ptr) |sptr| {
            const shard = shardFromPtr(sptr);
            // Carry the partition in the value prefix so it survives restart.
            const stored_value = stream_mod.encodeAppendValue(self.allocator, partition_index, payload_value) catch {
                return .{ .err = .{ .code = .internal_error, .message = "stream encode failed" } };
            };
            defer self.allocator.free(stored_value);
            // The entry is stamped with its namespace's id, so a namespace
            // nobody created is created first.
            shard.namespace_handler.proposeImplicitCreate(req.namespace, shard);
            const proposed = persistence_mod.proposeEntry(shard, .stream_append, entry_mod.Flags.NONE, req.namespace, req.key, stored_value) catch |err| {
                return .{ .err = persistence_mod.failureError(err, "append not persisted") };
            };
            return .{ .parked = proposed };
        }
        const stream_id = self.applyLocalStreamAppend(req, payload_value, partition_index) catch {
            return .{ .err = .{ .code = .internal_error, .message = "stream apply failed" } };
        };
        return appendOk(stream_id);
    }

    fn appendOk(stream_id: StreamID) CommandResult {
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
        // A stream that was never created (by create or a first append), or
        // was deleted. One that exists with nothing in the window answers
        // empty.
        if (req.key.len > ns_keys.MAX_QUALIFIED_KEY) return NAME_TOO_LONG;
        if (self.metaOf(req) == null) return .{ .err = .{ .code = .not_found, .message = NO_STREAM } };

        var window = self.readWindow(req);
        var buf: [MAX_READ_BATCH]StreamRecord = undefined;
        const records, const result = self.readReadable(&window, &buf);

        if (self.metrics_registry) |mr| {
            if (result == .stream_messages) if (mr.registerStream(req.namespace, req.key, 0)) |sm| {
                // Each record is one append; sum their batch contents so the
                // counter matches what the caller actually receives.
                var n: u64 = 0;
                for (records) |rec| n += rec.record_count;
                if (n == 0) sm.recordEmptyRead() else sm.recordRead(n, result.stream_messages.data.len);
            } else |_| {};
        }
        return result;
    }

    /// What a read request covers. `stream_start` is exclusive: a reader
    /// passes back the last id it saw and gets what came after it.
    pub fn readWindow(self: *StreamHandler, req: Request) waiter_pool_mod.StreamWindow {
        var window: waiter_pool_mod.StreamWindow = .{
            .stream = stream_mod.streamKey(req.ns_id, req.key),
            .limit = readLimit(req),
        };

        if (req.findOption(.stream_tail) != null) {
            window.after = self.stream.streamLastId(window.stream);
        } else if (req.findOption(.stream_start)) |opt| {
            if (opt.asStreamId()) |sid| {
                if (sid.timestamp_ms > 0) {
                    window.after = .{ .timestamp_ms = sid.timestamp_ms, .sequence = sid.sequence };
                } else if (sid.sequence > 0) {
                    // Bare sequence — convert to StreamID for lookup
                    window.after = StreamID.fromSeq(sid.sequence);
                }
            }
        }

        if (req.findOption(.stream_end)) |opt| {
            if (opt.asStreamId()) |sid| {
                if (sid.timestamp_ms > 0) {
                    window.end = .{ .timestamp_ms = sid.timestamp_ms, .sequence = sid.sequence };
                }
            }
        }

        if (req.findOption(.partition)) |opt| {
            window.partition = opt.asU32();
        } else if (req.findOption(.partition_key)) |opt| {
            const pk_bytes = opt.data;
            if (pk_bytes.len > 0) {
                const pc = stream_machine.partitionCount(self.readView(), req.ns_id, req.key);
                window.partition = @intCast(std.hash.Wyhash.hash(0, pk_bytes) % pc);
            }
        }
        return window;
    }

    /// The `count` option in records, bounded; absent or zero means the
    /// default. A read returns whole appends, so it can exceed this when the
    /// first append alone does.
    fn readLimit(req: Request) u32 {
        const asked = req.getCount() orelse 0;
        return @intCast(if (asked == 0) DEFAULT_READ_BATCH else @min(asked, MAX_READ_BATCH));
    }

    /// The records in `window`, at most `window.limit` of them.
    pub fn readRecords(self: *StreamHandler, window: waiter_pool_mod.StreamWindow, buf: *[MAX_READ_BATCH]StreamRecord) []StreamRecord {
        // Every append holds at least one record, so `limit` appends cover
        // the record budget; then keep the whole appends that fit it.
        const out = buf[0..window.limit];
        const count = if (window.end.eql(StreamID.MAX))
            self.stream.readStreamAfter(window.stream, window.after, window.partition, out)
        else
            self.stream.readStreamRange(window.stream, window.after, window.end, window.partition, out);
        return out[0..stream_mod.wholeEntriesWithin(out[0..count], window.limit)];
    }

    /// The window's records and their answer, past any appends that can't
    /// be read: an answer with none of a read's records would leave the
    /// reader's cursor where it was, and a follower re-reading them forever.
    ///
    /// The answer stops before a record being read back from disk: what the
    /// shard holds goes out, and the rest is the next read's. Only a read
    /// that holds none of its window is `.pending`, to be held until the
    /// disk read is back (or refused retryable, when `held` is `.blocked`).
    pub fn readReadable(self: *StreamHandler, window: *waiter_pool_mod.StreamWindow, buf: *[MAX_READ_BATCH]StreamRecord) struct { []StreamRecord, CommandResult } {
        self.beginRead();
        self.disk_window.stream = window.stream;
        while (true) {
            const records = self.readRecords(window.*, buf);
            const result = self.messages(records, window.after);
            const served = records[0..self.served_records];
            if (served.len == 0 and self.held != .no) {
                self.freeResult(result);
                return .{ served, .pending };
            }
            if (records.len == 0 or result != .stream_messages) return .{ served, result };
            if (std.mem.readInt(u32, result.stream_messages.data[0..4], .little) > 0) return .{ served, result };
            self.freeResult(result);
            window.after = stream_mod.lastRecordOf(served[served.len - 1]);
        }
    }

    /// What a read refused for its disk reads is told, by why.
    pub fn diskRefusal(self: *const StreamHandler) []const u8 {
        if (self.held != .blocked) return shard_mod.DISK_READ_PENDING;
        const d = self.disk orelse return shard_mod.DISK_READ_BLOCKED;
        return switch (d.want_block) {
            .io => shard_mod.DISK_READ_BLOCKED,
            .full => shard_mod.DISK_READ_CACHE_FULL,
            .mismatch => shard_mod.DISK_READ_MISMATCH,
            .unwritten => shard_mod.DISK_READ_UNWRITTEN,
        };
    }

    /// Before a read: nothing held yet.
    pub fn beginRead(self: *StreamHandler) void {
        self.held = .no;
        self.served_records = 0;
    }

    /// How many of a stream's records `ids`, in order, have their payloads
    /// in hand: held hot, warm or in the disk cache. At the first that isn't,
    /// its disk read is asked for and the prefix stops; `held` says whether
    /// it is coming (`pending`) or can't be read now (`blocked`).
    pub fn prefetch(self: *StreamHandler, ns: router.NamespaceId, stream_name: []const u8, ids: []const StreamID) usize {
        self.beginRead();
        const key = stream_mod.streamKey(ns, stream_name);
        self.disk_window.stream = key;
        for (ids, 0..) |id, i| {
            const rec = self.stream.streamAppendOf(key, id) orelse continue;
            self.disk_window.from = rec.ual_index;
            self.disk_window.limit = @intCast(ids.len - i);
            if (self.getPayloadAndTier(rec) == null and self.held != .no) return i;
        }
        return ids.len;
    }

    /// A read response carrying `records`. The caller frees it.
    pub fn messages(self: *StreamHandler, records: []const StreamRecord, after: StreamID) CommandResult {
        const data = self.serializeStreamRecordsWithPayloads(records, after, null) catch {
            return .{ .err = .{ .code = .internal_error, .message = "read serialization failed" } };
        };
        // Up to the first record still being read back, if any.
        const served = records[0..self.served_records];
        const last_id = if (served.len > 0) stream_mod.lastRecordOf(served[served.len - 1]) else StreamID.MIN;
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

        const key = stream_mod.streamKey(req.ns_id, req.key);

        if (req.value.len > 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "stream trim takes no value; give before, maxlen or maxage" } };
        }
        // Options a trim might be sent but doesn't take: refused by name, not ignored.
        inline for (.{
            .{ proto.OptionTag.max_bytes, "stream trim: max_bytes is not supported; give one of before, maxlen or maxage" },
            .{ proto.OptionTag.stream_end, "stream trim: stream_end is not a trim bound; give one of before, maxlen or maxage" },
            .{ proto.OptionTag.retention_count, "stream trim: retention_count is not a trim bound; send limit (maxlen)" },
            .{ proto.OptionTag.retention_age, "stream trim: retention_age is not a trim bound; send max_age_seconds (maxage)" },
            .{ proto.OptionTag.retention_bytes, "stream trim: retention_bytes is not supported; give one of before, maxlen or maxage" },
        }) |refused| {
            if (req.findOption(refused[0]) != null) return .{ .err = .{ .code = .invalid_request, .message = refused[1] } };
        }
        const dry_run = req.findOption(.dry_run) != null;

        const before = req.findOption(.stream_start);
        const maxlen = req.findOption(.limit);
        const maxage = req.findOption(.max_age_seconds);
        const bounds = @as(u8, @intFromBool(before != null)) + @intFromBool(maxlen != null) + @intFromBool(maxage != null);
        if (bounds == 0) return .{ .err = .{ .code = .invalid_request, .message = "stream trim: give one of before, maxlen or maxage" } };
        if (bounds > 1) return .{ .err = .{ .code = .invalid_request, .message = "stream trim: give only one of before, maxlen or maxage" } };

        // What goes: records up to a boundary id (see `trimStream`), or
        // records older than an age at the trim entry's stamp.
        var bound: TrimBound = undefined;
        if (before) |opt| {
            const sid = opt.asStreamId() orelse return .{ .err = .{ .code = .invalid_request, .message = "stream trim: before must be a stream id" } };
            if (sid.timestamp_ms > 0) {
                bound = .{ .id = .{ .timestamp_ms = sid.timestamp_ms, .sequence = sid.sequence } };
            } else if (sid.sequence > 0) {
                bound = .{ .id = StreamID.fromSeq(sid.sequence) };
            } else {
                return .{ .err = .{ .code = .invalid_request, .message = "stream trim: before must be after 0-0" } };
            }
        } else if (maxlen) |opt| {
            // Keep only the newest N records.
            const keep = opt.asU64() orelse return .{ .err = .{ .code = .invalid_request, .message = "stream trim: maxlen must be a u64" } };
            if (keep == 0) return .{ .err = .{ .code = .invalid_request, .message = "stream trim: maxlen must be > 0" } };
            // Logical records: counting entries would read a batched stream
            // as far shorter than it is.
            const total = self.stream.streamLogicalCount(key);
            if (total <= keep) return trimmed(key, 0, self.stream);
            const nth = self.stream.resolveNthRecordId(key, total - keep);
            // Trim cuts whole batches; when the cut falls inside the first
            // one, nothing can go without dropping records inside the window.
            if (nth.eql(StreamID.MIN)) return trimmed(key, 0, self.stream);
            bound = .{ .id = nth };
        } else if (maxage) |opt| {
            // Remove records older than S seconds, by the trim entry's stamp.
            const age_s = opt.asU64() orelse return .{ .err = .{ .code = .invalid_request, .message = "stream trim: maxage must be a u64" } };
            if (age_s == 0) return .{ .err = .{ .code = .invalid_request, .message = "stream trim: maxage must be > 0" } };
            if (age_s > time_units.MAX_AGE_MS / std.time.ms_per_s) return .{ .err = .{ .code = .invalid_request, .message = "stream trim: maxage is over 100 years" } };
            bound = .{ .age_ms = age_s * std.time.ms_per_s };
            // Nothing that old by this node's time: no entry to propose.
            if (bound.resolve(self.nowNs()) == null) return trimmed(key, 0, self.stream);
        } else unreachable;

        // A dry run answers from the projection on the owning shard: what the
        // trim would remove and the first record it would leave, with no
        // proposal and no log entry.
        if (dry_run) {
            const trim_id = bound.resolve(self.nowNs()) orelse return .{ .stream_trimmed = .{ .deleted_count = 0, .first_seq = firstSeq(self.stream, key) } };
            const first_id = self.stream.firstIdAfterTrim(key, trim_id);
            return .{ .stream_trimmed = .{
                .deleted_count = self.stream.trimCount(key, trim_id),
                .first_seq = if (first_id.eql(StreamID.MIN)) 0 else first_id.sequence,
            } };
        }

        if (self.shard_ptr) |sptr| {
            const proposed = self.proposeTrim(shardFromPtr(sptr), key, bound) catch |err| {
                return .{ .err = persistence_mod.failureError(err, "trim not persisted") };
            };
            return .{ .parked = proposed };
        }
        // Without a shard (unit tests), the machine applies it here.
        var key_buf: [TrimBound.MAX_SIZE]u8 = undefined;
        const t = trimCommand(&key, bound, &key_buf);
        const out = self.applyLocal(.stream_trim, entry_mod.Flags.NONE, t.ns, t.key, t.value) catch
            return .{ .err = .{ .code = .internal_error, .message = "stream trim not applied" } };
        var r = out.reader();
        return .{ .stream_trimmed = .{ .deleted_count = r.int(u64) orelse 0, .first_seq = r.int(u64) orelse 0 } };
    }

    fn trimmed(key: StreamKey, deleted: u64, stream: *StreamProjection) CommandResult {
        const first_id = stream.streamFirstId(key);
        const first_seq = if (!first_id.eql(StreamID.MIN)) first_id.sequence else 0;
        return .{ .stream_trimmed = .{
            .deleted_count = deleted,
            .first_seq = first_seq,
        } };
    }

    pub const NOT_EMPTY_MESSAGE = stream_machine.NOT_EMPTY;

    // ── DELETE ──────────────────────────────────────────────────────────

    /// Delete a stream, logged: the streams machine removes its appends,
    /// retention and `stream.meta` record in the entry's apply (or leaves a
    /// tombstone at the next generation while groups count it, so they read
    /// as gone), and its metrics series goes after apply. Deleting a missing
    /// stream is ok.
    /// Refuses a non-empty stream unless `force` (option 0x2C) is set.
    fn handleDelete(self: *StreamHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "stream name is required" } };
        }

        const key = stream_mod.streamKey(req.ns_id, req.key);

        // Whether the stream is empty is decided as the entry applies: here, an
        // append committed and not yet applied would read as nothing.
        const force = req.findOption(.force) != null;
        if (self.shard_ptr) |sptr| {
            const proposed = self.proposeDelete(shardFromPtr(sptr), req.ns_id, req.key, force) catch |err| {
                return .{ .err = persistence_mod.failureError(err, "delete not persisted") };
            };
            return .{ .parked = proposed };
        }
        _ = key;
        if (req.key.len > ns_keys.MAX_QUALIFIED_KEY) return NAME_TOO_LONG;
        const out = self.applyLocal(.stream_delete, if (force) entry_mod.Flags.FORCE else entry_mod.Flags.NONE, req.ns_id, req.key, "") catch
            return .{ .err = .{ .code = .internal_error, .message = "stream delete not applied" } };
        if (out.refused() != null) return .{ .err = .{ .code = .conflict, .message = NOT_EMPTY_MESSAGE } };
        return .ok;
    }

    // ── INFO ────────────────────────────────────────────────────────────

    fn handleInfo(self: *StreamHandler, req: Request) CommandResult {
        const key = stream_mod.streamKey(req.ns_id, req.key);
        if (req.key.len > ns_keys.MAX_QUALIFIED_KEY) return NAME_TOO_LONG;
        // An unknown stream is not an empty one.
        const meta = self.metaOf(req) orelse return .{ .err = .{ .code = .not_found, .message = NO_STREAM } };

        const first_id = self.stream.streamFirstId(key);
        const last_id = self.stream.streamLastId(key);
        // Logical records (batch contents), not append entries.
        const count = self.stream.streamLogicalCount(key);
        const bytes = self.stream.streamByteSize(key);

        return .{ .stream_info = .{
            .first_timestamp_ms = if (count > 0) first_id.timestamp_ms else 0,
            .first_seq = if (count > 0) first_id.sequence else 0,
            .last_timestamp_ms = if (count > 0) last_id.timestamp_ms else 0,
            .last_seq = if (count > 0) last_id.sequence else 0,
            .count = count,
            .bytes = bytes,
            .partition_count = @max(1, meta.partition_count),
            .retention_age_s = meta.retention_age_s,
            .retention_count = meta.retention_count,
            .retention_bytes = 0,
        } };
    }

    // ── CREATE ──────────────────────────────────────────────────────────

    /// The request's stream's name key, which keys its listing and its
    /// settings (partition count, which drives routing, and retention): by
    /// the bare name, a same-named stream in another namespace would share
    /// them.
    fn nameKey(buf: *[stream_mod.MAX_NAME_KEY]u8, req: Request) ?[]const u8 {
        return stream_mod.nameKey(buf, req.ns_id, req.key);
    }

    fn handleCreate(self: *StreamHandler, req: Request) CommandResult {
        if (req.findOption(.retention_bytes) != null) return BYTE_RETENTION_REFUSAL;
        if (req.findOption(.retention_age_clear) != null or req.findOption(.retention_count_clear) != null)
            return .{ .err = .{ .code = .invalid_request, .message = stream_machine.CLEAR_ON_CREATE } };
        if (retentionRefusal(req)) |refused| return refused;
        if (req.key.len == 0) return .{ .err = .{ .code = .invalid_request, .message = "stream name is required" } };
        var partition_count: u32 = 1;
        if (req.value.len >= 4) {
            var reader = WireReader.init(req.value);
            if (reader.readU32()) |pc| {
                if (pc > 0) partition_count = pc;
            }
        }
        const retention = parseRetentionOptions(req);
        return self.proposeConfig(req, .{ .op = .create, .partition_count = partition_count, .retention_age_s = retention.age_s, .retention_count = retention.count });
    }

    // ── ALTER ────────────────────────────────────────────────

    fn handleAlter(self: *StreamHandler, req: Request) CommandResult {
        if (req.key.len == 0) return .{ .err = .{ .code = .invalid_request, .message = "stream name is required" } };
        if (req.findOption(.retention_bytes) != null) return BYTE_RETENTION_REFUSAL;
        if (retentionRefusal(req)) |refused| return refused;
        const retention = parseRetentionOptions(req);
        // A bound of 0 leaves the stream's as it is; a clear drops it.
        const clear_age = req.findOption(.retention_age_clear) != null;
        const clear_count = req.findOption(.retention_count_clear) != null;
        if ((clear_age and retention.age_s > 0) or (clear_count and retention.count > 0))
            return .{ .err = .{ .code = .invalid_request, .message = "bad request: an alter both sets and clears a retention bound" } };
        return self.proposeConfig(req, .{
            .op = .alter,
            .partition_count = 0,
            .retention_age_s = if (clear_age) StreamConfig.CLEAR else retention.age_s,
            .retention_count = if (clear_count) StreamConfig.CLEAR else retention.count,
        });
    }

    /// A create or an alter, logged so every replica holds the stream's
    /// partition count and retention, and a restart or a new leader keeps
    /// them. The applier decides; the client is answered when it applies.
    fn proposeConfig(self: *StreamHandler, req: Request, config: StreamConfig) CommandResult {
        if (req.key.len > ns_keys.MAX_QUALIFIED_KEY) return NAME_TOO_LONG;
        // An alter of a stream that doesn't exist would be refused at
        // apply: refused here, without the commit.
        if (config.op == .alter and self.metaOf(req) == null) return .{ .err = .{ .code = .not_found, .message = NO_STREAM } };
        var value: [StreamConfig.SIZE]u8 = undefined;
        config.encode(&value);
        const sptr = self.shard_ptr orelse {
            // Without a shard (unit tests), the machine applies it here.
            var payload_buf: [persistence_mod.MAX_PERSIST_PAYLOAD]u8 = undefined;
            const entry = entry_mod.buildCommandEntry(.stream_config, entry_mod.Flags.NONE, 0, self.partition.committed_index + 1, 0, req.ns_id, req.key, &value, &payload_buf) orelse return NAME_TOO_LONG;
            var out = Result.start(&entry);
            contract_machine.applyEntry(&stream_machine.machine, &self.partition.store, &self.partition.apply_scratch, &entry, &out) catch
                return .{ .err = .{ .code = .internal_error, .message = "the stream's settings were not applied" } };
            const r = out.refused() orelse return .ok;
            return .{ .err = .{ .code = switch (r.status) {
                .not_found => .not_found,
                .conflict => .already_exists,
                else => .invalid_request,
            }, .message = r.refusal.message } };
        };
        const proposed = persistence_mod.proposeEntry(shardFromPtr(sptr), .stream_config, entry_mod.Flags.NONE, req.namespace, req.key, &value) catch |err| {
            return .{ .err = .{ .code = persistence_mod.failureCode(err), .message = persistence_mod.failureMessage(err, "the stream's settings were not persisted") } };
        };
        return .{ .parked = proposed };
    }

    const NO_STREAM = stream_machine.NO_STREAM;

    pub const StreamConfig = stream_machine.StreamConfig;

    /// What this shard has applied of its streams' metadata.
    fn readView(self: *const StreamHandler) ReadView {
        return .{ .store = &self.partition.store };
    }

    /// Without a shard (unit tests): an entry applied here through the
    /// streams machine, as the shard's apply runs it, stamped now.
    fn applyLocal(self: *StreamHandler, etype: EntryType, flags: u16, ns: router.NamespaceId, key: []const u8, value: []const u8) !Result {
        var payload_buf: [persistence_mod.MAX_PERSIST_PAYLOAD]u8 = undefined;
        const entry = entry_mod.buildCommandEntry(etype, flags, 0, self.partition.committed_index + 1, self.nowNs(), ns, key, value, &payload_buf) orelse return error.NameTooLong;
        var out = Result.start(&entry);
        try contract_machine.applyEntry(&stream_machine.machine, &self.partition.store, &self.partition.apply_scratch, &entry, &out);
        return out;
    }

    /// Which stream `name` in `ns` is, or null where there is none: the
    /// stamp of the entry that made it. A reader resuming a cursor checks
    /// it: a stream deleted and made again has another (stamps only rise).
    /// Its ids continue past the old stream's while the old one's tombstone
    /// is held and start again once it is forgotten, so ids alone can't
    /// tell an old cursor from a new one.
    pub fn madeAt(self: *const StreamHandler, ns: router.NamespaceId, name: []const u8) ?u64 {
        return (stream_machine.metaOf(self.readView(), ns, name) orelse return null).created_ns;
    }

    /// The request's stream, if it exists.
    fn metaOf(self: *const StreamHandler, req: Request) ?stream_machine.Meta.Value {
        return stream_machine.metaOf(self.readView(), req.ns_id, req.key);
    }

    /// Retention enforces age and count only; a byte bound would be stored
    /// and never applied.
    const BYTE_RETENTION_REFUSAL: CommandResult = .{ .err = .{ .code = .invalid_request, .message = "stream retention by bytes is not supported; use retention_age or retention_count" } };

    /// A retention age past 100 years is a mistake, not a policy: refused
    /// rather than left to trim nothing.
    fn retentionRefusal(req: Request) ?CommandResult {
        const r = parseRetentionOptions(req);
        if (r.age_s > time_units.MAX_AGE_MS / std.time.ms_per_s) return .{ .err = .{ .code = .invalid_request, .message = "stream retention age is over 100 years" } };
        // The logged entry carries a cleared bound as this value: a client's
        // own would clear the bound it meant to set.
        if (r.count == StreamConfig.CLEAR) return .{ .err = .{ .code = .invalid_request, .message = "retention bound too large" } };
        return null;
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

    // ── Helpers ─────────────────────────────────────────────────────────

    /// An append a read can't return: counted and logged once.
    fn noteUnreadable(self: *StreamHandler, rec: StreamRecord) void {
        // Once per append: by (index, part), in that order.
        if (rec.ual_index < self.highest_skipped_ual or (rec.ual_index == self.highest_skipped_ual and rec.part <= self.highest_skipped_part)) return;
        self.highest_skipped_ual = rec.ual_index;
        self.highest_skipped_part = rec.part;
        self.skipped_appends += 1;
        if (self.shard_ptr) |sptr| if (shardFromPtr(sptr).shard_metrics) |sm| sm.recordStreamSkippedAppend();
        log.err("stream: the append at log index {d} (part {d}) is in neither hot nor warm storage, or isn't a batch; its records are skipped", .{ rec.ual_index, rec.part });
    }

    /// The id of record `j` of an append.
    fn recordId(rec: StreamRecord, j: u64) StreamID {
        return .{ .timestamp_ms = rec.id.timestamp_ms, .sequence = rec.id.sequence + j };
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
    /// Records at or before `after` are left out: a cursor inside an append
    /// resumes with the rest of it.
    /// `readable`, if given, is set per append: whether its records went in.
    pub fn serializeStreamRecordsWithPayloads(self: *StreamHandler, records: []const StreamRecord, after: StreamID, readable: ?[]bool) ![]u8 {
        // The reader named the stream; repeating it per record would let a
        // long name push one append's answer past a frame.
        const key_size: usize = 1;

        // Every record of every append in `records`, never part of one: a
        // reader's cursor (or a group's delivered id) passes whole appends.
        // Each append's blob is read once and walked twice, to size and to write.
        std.debug.assert(records.len <= MAX_READ_BATCH);
        var blobs: [MAX_READ_BATCH]?stream_mod.BatchIterator = undefined;
        var tiers: [MAX_READ_BATCH]u8 = undefined;
        var count: usize = 0;
        var total: usize = 4; // count header
        // Up to the first record being read back from disk: the answer is
        // the in-order prefix the shard holds, so a reader always moves on,
        // and the rest waits for the next read.
        var served = records.len;
        for (records, 0..) |rec, i| {
            self.disk_window.from = rec.ual_index;
            self.disk_window.limit = @intCast(records.len - i);
            const r = self.getPayloadAndTier(rec);
            if (r == null and self.held != .no) {
                served = i;
                break;
            }
            tiers[i] = if (r) |p| @backingInt(p.tier) else 0;
            blobs[i] = if (r) |p| stream_mod.BatchIterator.init(p.payload) catch null else null;
            if (readable) |rd| rd[i] = blobs[i] != null;
            var it = blobs[i] orelse {
                self.noteUnreadable(rec);
                continue;
            };
            var j: u64 = 0;
            while (it.next()) |br| : (j += 1) {
                if (!recordId(rec, j).greaterThan(after)) continue;
                count += 1;
                total += 8 + 8 + 1 + 4 + key_size + 4 + br.payload.len + 4 + responseHeaderSize(br.header_count, br.headers_raw.len);
            }
        }

        const buf = try self.allocator.alloc(u8, total);
        errdefer self.allocator.free(buf);

        var pos: usize = 0;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(count), .little);
        pos += 4;

        self.served_records = served;
        for (records[0..served], 0..) |rec, i| {
            var it = blobs[i] orelse continue;
            var j: u64 = 0;
            while (it.next()) |br| : (j += 1) {
                if (!recordId(rec, j).greaterThan(after)) continue;
                std.mem.writeInt(u64, buf[pos..][0..8], rec.id.sequence + j, .little);
                pos += 8;
                std.mem.writeInt(i64, buf[pos..][0..8], @as(i64, @intCast(rec.id.timestamp_ms)), .little);
                pos += 8;
                buf[pos] = tiers[i];
                pos += 1;
                std.mem.writeInt(u32, buf[pos..][0..4], rec.partition_index, .little);
                pos += 4;

                buf[pos] = 0; // key_present
                pos += 1;

                std.mem.writeInt(u32, buf[pos..][0..4], @intCast(br.payload.len), .little);
                pos += 4;
                @memcpy(buf[pos .. pos + br.payload.len], br.payload);
                pos += br.payload.len;

                std.mem.writeInt(u32, buf[pos..][0..4], br.header_count, .little);
                pos += 4;
                if (br.header_count > 0) {
                    pos = writeResponseHeaders(br.headers_raw, br.header_count, buf, pos);
                }
            }
        }
        std.debug.assert(pos == total);

        return buf;
    }

    /// A group read's answer: `[term:u64][index:u64][expires_at_ms:i64]`
    /// Why a group read's answer left a record out.
    pub const Unread = enum(u8) {
        read,
        /// Its payload is on its way back from disk: no fault of the record.
        fetching,
        /// Its payload is gone or malformed: it can't be read again.
        lost,
    };

    /// `[count:u32]`, then each record in the order its deliver took them, as
    /// a stream read gives it, followed by its `[deliveries:u32]`. A record
    /// whose payload can't be read now is left out, and `unread` says why.
    pub fn serializeGroupRecords(self: *StreamHandler, key: StreamKey, term: u64, index: u64, expires_at_ms: i64, records: []const StreamID, deliveries: []const u32, unread: []Unread) ![]u8 {
        const key_size: usize = 1;
        const Found = struct { rec: StreamRecord, br: stream_mod.BatchRecord, tier: proto.StorageTier };
        const found = try self.allocator.alloc(?Found, records.len);
        defer self.allocator.free(found);
        var total: usize = group_handler.READ_FIXED;
        var count: u32 = 0;
        for (records, found, 0..) |id, *f, i| {
            f.* = null;
            unread[i] = .lost;
            const rec = self.stream.streamAppendOf(key, id) orelse continue;
            // Asked of the disk now, or waiting on it: on its way back.
            const before = self.held;
            self.held = .no;
            const got = self.getPayloadAndTier(rec);
            const fetching = self.held != .no;
            if (before == .pending or (before == .blocked and self.held == .no)) self.held = before;
            const r = got orelse {
                if (fetching) unread[i] = .fetching else self.noteUnreadable(rec);
                continue;
            };
            var it = stream_mod.BatchIterator.init(r.payload) catch {
                self.noteUnreadable(rec);
                continue;
            };
            var j: u64 = 0;
            const want = id.sequence - rec.id.sequence;
            while (it.next()) |br| : (j += 1) if (j == want) {
                f.* = .{ .rec = rec, .br = br, .tier = r.tier };
                break;
            };
            const g = f.* orelse continue;
            unread[i] = .read;
            total += 8 + 8 + 1 + 4 + key_size + 4 + g.br.payload.len + 4 + responseHeaderSize(g.br.header_count, g.br.headers_raw.len) + 4;
            count += 1;
        }
        const buf = try self.allocator.alloc(u8, total);
        errdefer self.allocator.free(buf);
        std.mem.writeInt(u64, buf[0..8], term, .little);
        std.mem.writeInt(u64, buf[8..16], index, .little);
        std.mem.writeInt(i64, buf[16..24], if (count == 0) 0 else expires_at_ms, .little);
        std.mem.writeInt(u32, buf[24..28], count, .little);
        var pos: usize = group_handler.READ_FIXED;
        for (records, found, deliveries) |id, f, d| {
            const g = f orelse continue;
            std.mem.writeInt(u64, buf[pos..][0..8], id.sequence, .little);
            pos += 8;
            std.mem.writeInt(i64, buf[pos..][0..8], @as(i64, @intCast(id.timestamp_ms)), .little);
            pos += 8;
            buf[pos] = @backingInt(g.tier);
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], g.rec.partition_index, .little);
            pos += 4;
            buf[pos] = 0; // key_present
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], @intCast(g.br.payload.len), .little);
            pos += 4;
            @memcpy(buf[pos .. pos + g.br.payload.len], g.br.payload);
            pos += g.br.payload.len;
            std.mem.writeInt(u32, buf[pos..][0..4], g.br.header_count, .little);
            pos += 4;
            if (g.br.header_count > 0) pos = writeResponseHeaders(g.br.headers_raw, g.br.header_count, buf, pos);
            std.mem.writeInt(u32, buf[pos..][0..4], d, .little);
            pos += 4;
        }
        std.debug.assert(pos == total);
        return buf;
    }

    /// The id of namespace `name` as the node's table has it: a pipeline or
    /// trigger names its source by name, and may read from another shard's
    /// thread, which the table allows and the shard's own maps don't. A
    /// namespace not yet published is `.invalid`, and reads as empty.
    fn namespaceId(self: *const StreamHandler, name: []const u8) router.NamespaceId {
        const effective = if (name.len == 0) "default" else name;
        if (std.mem.eql(u8, effective, "default")) return .default;
        const sptr = self.shard_ptr orelse return .invalid;
        return shardFromPtr(sptr).namespace_handler.table.lookup(effective) orelse .invalid;
    }

    /// The name of namespace `id`, as the node's table has it, or null
    /// while the table doesn't have it yet.
    fn namespaceName(self: *const StreamHandler, id: router.NamespaceId) ?[]const u8 {
        if (id == .default) return "default";
        const sptr = self.shard_ptr orelse return null;
        return shardFromPtr(sptr).namespace_handler.table.nameOf(id);
    }

    /// Append one record of `payload` to a named stream, as a client's
    /// append would; tests write their sources with it.
    pub fn appendPayloadToStream(self: *StreamHandler, stream_name: []const u8, namespace: []const u8, payload: []const u8) !persistence_mod.ProposeResult {
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
        // Pipeline appends are single-partition; carry partition 0 in the value
        // prefix so the entry decodes consistently on replay.
        const stored_value = try stream_mod.encodeAppendValue(self.allocator, 0, batch_value);
        defer self.allocator.free(stored_value);
        // The applier registers the stream's name; the caller proposed the
        // namespace's implicit create first where it could.
        return persistence_mod.proposeEntry(shard, .stream_append, entry_mod.Flags.SINK, namespace, stream_name, stored_value);
    }

    /// Read payloads from a named stream (used by processing pipelines).
    /// Uses per-stream StreamID reads for proper stream isolation.
    /// Returns (payloads slice, last StreamID read) so the pipeline can advance its cursor.
    pub const PayloadRead = struct {
        payloads: []const []const u8,
        /// The StreamID each payload came from, parallel to `payloads`.
        ids: []const StreamID,
        last_id: StreamID,

        pub fn deinit(self: PayloadRead, allocator: Allocator) void {
            allocator.free(self.payloads);
            allocator.free(self.ids);
        }
    };

    pub fn readPayloadsForStream(self: *StreamHandler, stream_name: []const u8, namespace: []const u8, after_id: StreamID, limit: usize) PayloadRead {
        return self.readPayloadsForStreamIn(stream_name, self.namespaceId(namespace), after_id, limit);
    }

    /// `readPayloadsForStream` for a caller that holds the namespace's id.
    pub fn readPayloadsForStreamIn(self: *StreamHandler, stream_name: []const u8, ns: router.NamespaceId, after_id: StreamID, limit: usize) PayloadRead {
        var results: [MAX_READ_BATCH][]const u8 = undefined;
        var result_ids: [MAX_READ_BATCH]StreamID = undefined;
        var count: usize = 0;
        const capped = @min(limit, MAX_READ_BATCH);

        const stream_key = stream_mod.streamKey(ns, stream_name);

        var rec_buf: [MAX_READ_BATCH]StreamRecord = undefined;
        const read_n = self.stream.readStreamAfter(stream_key, after_id, null, rec_buf[0..capped]);
        // Whole appends only, so `last_id` never lands inside one: resuming
        // after it would skip the rest of that append.
        const n = stream_mod.wholeEntriesWithin(rec_buf[0..read_n], capped);

        var last_id = after_id;
        self.beginRead();
        self.disk_window.stream = stream_key;
        // The payloads are good until the shard's next tick lands disk reads,
        // after this poll has used them.
        for (rec_buf[0..n], 0..) |rec, i| {
            self.disk_window.from = rec.ual_index;
            self.disk_window.limit = @intCast(n - i);
            const result = self.getPayloadAndTier(rec);
            // Being read from disk: stop before it, and the next poll, once
            // the read is back, resumes there.
            if (result == null and self.held != .no) break;
            var it = if (result) |p| stream_mod.BatchIterator.init(p.payload) catch null else null;
            if (it == null) self.noteUnreadable(rec);
            if (it) |*batch| {
                var j: u64 = 0;
                while (batch.next()) |br| : (j += 1) {
                    if (!recordId(rec, j).greaterThan(after_id)) continue;
                    results[count] = br.payload;
                    // Each record its own id: a reader keying on it (a
                    // trigger's idempotency key) must tell them apart.
                    result_ids[count] = recordId(rec, j);
                    count += 1;
                }
            }
            last_id = stream_mod.lastRecordOf(rec);
        }

        // Out of memory: nothing read, so the cursor stays put.
        const out = self.allocator.alloc([]const u8, count) catch return .{ .payloads = &.{}, .ids = &.{}, .last_id = after_id };
        const ids = self.allocator.alloc(StreamID, count) catch {
            self.allocator.free(out);
            return .{ .payloads = &.{}, .ids = &.{}, .last_id = after_id };
        };
        @memcpy(out, results[0..count]);
        @memcpy(ids, result_ids[0..count]);
        return .{ .payloads = out, .ids = ids, .last_id = last_id };
    }

    /// A record's batch blob by its locator (`ual_index`, `part`): from the
    /// hot ring (zero-copy), then the warm store, then the durable log
    /// (`disk`). Every tier hands `stream_mod.appendAt` its entry's payload,
    /// which strips the partition prefix and, for a channel message, the
    /// inbound around it: no reader decodes a stored append itself. Null when it is
    /// in none, and the readers skip and count it; or when it is being read
    /// back (`held` says so), and the readers stop before it.
    ///
    /// Wrap-safety: the zero-copy `read` below returns null for an entry whose
    /// payload wraps the hot-ring boundary. That is SAFE here precisely because
    /// `partition.apply` copies every applied payload into the warm store, so the
    /// warm fallback serves the wrapped entry as tier=1. This is the canonical
    /// "null is OK, fall through" use of `read` — do not switch it to readCopy
    /// (that would force a copy + a response-lived buffer for no correctness gain).
    fn getPayloadAndTier(self: *StreamHandler, rec: StreamRecord) ?struct { payload: []const u8, tier: proto.StorageTier } {
        const ual_index = rec.ual_index;
        // Hot path — entry still in the UAL ring buffer.
        if (self.partition.ual.read(ual_index)) |ual_entry| {
            // Only here is the entry's type at hand: a part past 0 names a
            // message, which only an inbound carries.
            std.debug.assert(rec.part == 0 or ual_entry.header.entry_type == @backingInt(entry_mod.EntryType.channel_inbound));
            if (stream_mod.appendAt(ual_entry.payload, rec.part)) |av| {
                if (self.tiered_metrics) |tm| tm.recordHotHit();
                return .{ .payload = av.payload, .tier = .hot };
            }
        }
        // Warm fallback — payload copied to partition warm store on apply()
        if (self.partition.readPayloadWarm(ual_index)) |raw| {
            if (stream_mod.appendAt(raw, rec.part)) |av| {
                if (self.tiered_metrics) |tm| tm.recordWarmHit();
                return .{ .payload = av.payload, .tier = .warm };
            }
        }
        // Then the durable log: its sealed segment, or the writer's buffer.
        if (self.disk) |d| {
            if (d.get(ual_index)) |raw| if (stream_mod.appendAt(raw, rec.part)) |av| {
                return .{ .payload = av.payload, .tier = .disk };
            };
            switch (d.want(ual_index, self.disk_window)) {
                .pending => {
                    self.held = .pending;
                    return null;
                },
                .blocked => {
                    if (self.held == .no) self.held = .blocked;
                    return null;
                },
                .absent => {},
            }
        }
        if (self.tiered_metrics) |tm| tm.recordMiss();
        return null;
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

    /// Apply a stream append through the same applier the shard uses, on a
    /// locally built entry. Only for when no shard is wired (unit tests). The
    /// UAL index is assigned from the partition's committed_index so it stays
    /// monotonic and lines up with lookups.
    fn applyLocalStreamAppend(self: *StreamHandler, req: Request, payload_value: []const u8, partition_index: u32) !StreamID {
        const timestamp_ns: u64 = @intCast(@as(u64, @bitCast(@as(i64, @import("stdx").time.milliTimestamp()))) * 1_000_000);
        const next_index = self.partition.committed_index + 1;

        // Carry the partition in the value prefix so it survives restart.
        const stored_value = try stream_mod.encodeAppendValue(self.allocator, partition_index, payload_value);
        defer self.allocator.free(stored_value);

        var payload_buf: [persistence_mod.MAX_PERSIST_PAYLOAD]u8 = undefined;
        const entry = entry_mod.buildCommandEntry(
            .stream_append,
            entry_mod.Flags.NONE,
            0, // term — unused without consensus
            next_index,
            timestamp_ns,
            req.ns_id,
            req.key,
            stored_value,
            &payload_buf,
        ) orelse return error.PayloadTooLarge;

        _ = try self.partition.apply(&entry);
        var out = Result.start(&entry);
        contract_machine.applyEntry(&stream_machine.machine, &self.partition.store, &self.partition.apply_scratch, &entry, &out) catch return error.AppendNotApplied;
        if (out.refused() != null) return error.AppendNotApplied;
        const answer = appendedFrom(&out).stream_append_ok;
        return .{ .sequence = answer.sequence, .timestamp_ms = @intCast(answer.timestamp_ms) };
    }

    // ── Trim Persistence ────────────────────────────────────────────────

    /// A trim entry, in the stream's namespace: key = the bound
    /// (`TrimBound.encode`), value = the stream key past the id, its name's
    /// hash.
    fn proposeTrim(self: *StreamHandler, shard: *Shard, key: StreamKey, bound: TrimBound) !persistence_mod.ProposeResult {
        _ = self;
        var key_buf: [TrimBound.MAX_SIZE]u8 = undefined;
        const t = trimCommand(&key, bound, &key_buf);
        return persistence_mod.proposeEntryIn(shard, .stream_trim, entry_mod.Flags.NONE, t.ns, t.key, t.value);
    }

    const TrimCommand = struct { ns: router.NamespaceId, key: []const u8, value: []const u8 };

    fn trimCommand(key: *const StreamKey, bound: TrimBound, key_buf: *[TrimBound.MAX_SIZE]u8) TrimCommand {
        return .{ .ns = stream_mod.keyNamespace(key), .key = bound.encode(key_buf), .value = key[4..12] };
    }

    /// A delete entry: key = the stream's name, in its namespace.
    fn proposeDelete(self: *StreamHandler, shard: *Shard, ns: router.NamespaceId, name: []const u8, force: bool) !persistence_mod.ProposeResult {
        _ = self;
        return persistence_mod.proposeEntryIn(shard, .stream_delete, if (force) entry_mod.Flags.FORCE else entry_mod.Flags.NONE, ns, name, "");
    }

    /// The retention duty's batch removal of a marked stream's appends at or
    /// below its floor (`stream_clean`), or its drop of a tombstone of
    /// generation `gen` (`stream_forget`): proposed, applying when it
    /// commits. Whether it was proposed.
    pub fn persistClean(self: *StreamHandler, key: StreamKey) bool {
        return self.persistDuty(.stream_clean, key, key[4..12]);
    }

    pub fn persistForget(self: *StreamHandler, key: StreamKey, gen: u64) bool {
        var value: [16]u8 = undefined;
        @memcpy(value[0..8], key[4..12]);
        std.mem.writeInt(u64, value[8..16], gen, .little);
        return self.persistDuty(.stream_forget, key, &value);
    }

    fn persistDuty(self: *StreamHandler, etype: EntryType, key: StreamKey, value: []const u8) bool {
        const ns = stream_mod.keyNamespace(&key);
        const sptr = self.shard_ptr orelse {
            _ = self.applyLocal(etype, entry_mod.Flags.NONE, ns, "", value) catch return false;
            return true;
        };
        _ = persistence_mod.proposeEntryIn(shardFromPtr(sptr), etype, entry_mod.Flags.NONE, ns, "", value) catch |err| {
            if (!shardFromPtr(sptr).quietHalted(err)) log.err("stream retention: {s} of stream {x} not persisted: {s}", .{ @tagName(etype), &key, @errorName(err) });
            return false;
        };
        return true;
    }

    /// Retention's trims: proposed; the trim applies when it commits.
    /// Whether it was proposed (without a shard, applied here).
    pub fn persistTrim(self: *StreamHandler, key: StreamKey, bound: TrimBound) bool {
        const sptr = self.shard_ptr orelse {
            var key_buf: [TrimBound.MAX_SIZE]u8 = undefined;
            const t = trimCommand(&key, bound, &key_buf);
            const out = self.applyLocal(.stream_trim, entry_mod.Flags.NONE, t.ns, t.key, t.value) catch return false;
            var r = out.reader();
            return (r.int(u64) orelse 0) > 0;
        };
        _ = self.proposeTrim(shardFromPtr(sptr), key, bound) catch |err| {
            if (!shardFromPtr(sptr).quietHalted(err)) log.err("stream retention: trim of stream {x} not persisted: {s}", .{ &key, @errorName(err) });
            return false;
        };
        return true;
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    /// This node's time, as its Raft node keeps it (the wall clock with no
    /// shard), for a leader deciding whether a trim would remove anything.
    fn nowNs(self: *const StreamHandler) u64 {
        if (self.shard_ptr) |sptr| return shardFromPtr(sptr).raft_node.now();
        return @intCast(@max(0, @import("stdx").time.nanoTimestamp()));
    }

    fn firstSeq(stream: *StreamProjection, key: StreamKey) u64 {
        const first_id = stream.streamFirstId(key);
        return if (first_id.eql(StreamID.MIN)) 0 else first_id.sequence;
    }

    /// Cast opaque shard pointer to a Shard-like type for proposing entries.
    /// Uses anytype to avoid circular import.
    fn shardFromPtr(ptr: *anyopaque) *Shard {
        return @ptrCast(@alignCast(ptr));
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
            shard.sendRefusal(conn, request_id, e.code.toStatus(), e.refusal());
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

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization
// ═══════════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

/// Helper to build a test request.
fn makeRequest(op: OpCode, key: []const u8, value: []const u8, options: []const u8) Request {
    return .{
        .header = proto.RequestHeader.init(op, 1),
        .namespace = "default",
        .ns_id = .default,
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

/// Count records a partition-filtered read returns for `stream`.
fn readPartitionCount(handler: *StreamHandler, stream: []const u8, partition_index: u32) !u32 {
    var opt_buf: [16]u8 = undefined;
    var builder = OptionsBuilder.init(&opt_buf);
    try builder.addU32(.partition, partition_index);
    const result = handler.handleCommand(makeRequest(.stream_read, stream, "", builder.getOptions()));
    defer handler.freeResult(result);
    return switch (result) {
        .stream_messages => |m| std.mem.readInt(u32, m.data[0..4], .little),
        else => error.TestUnexpectedResult,
    };
}

/// `e` applied as the shard applies it: through the streams machine.
fn applyLogged(h: *StreamHandler, e: *const entry_mod.Entry) persistence_mod.ApplyError!Result {
    var out = Result.start(e);
    try contract_machine.applyEntry(&stream_machine.machine, &h.partition.store, &h.partition.apply_scratch, e, &out);
    return out;
}

/// An append of `count` records to `ns`'s stream `name`, at log index
/// `index` and `ms`, applied through the streams machine; its payload is
/// in no tier unless the test puts it there.
fn appendRecord(h: *StreamHandler, ns: router.NamespaceId, name: []const u8, index: u64, ms: u64, count: u32) !StreamID {
    var vb: [512]u8 = undefined;
    var pb: [1024]u8 = undefined;
    const e = entry_mod.buildCommandEntry(.stream_append, entry_mod.Flags.NONE, 1, index, ms * std.time.ns_per_ms, ns, name, stream_mod.appendValueOf(&vb, count, 4), &pb).?;
    var out = try applyLogged(h, &e);
    try testing.expect(out.refused() == null);
    var r = out.reader();
    const seq = r.int(u64).?;
    return .{ .timestamp_ms = r.int(u64).?, .sequence = seq };
}

/// The stream `name` in the default namespace, as the handler reads it.
fn metaFor(h: *const StreamHandler, name: []const u8) ?stream_machine.Meta.Value {
    return stream_machine.metaOf(h.readView(), .default, name);
}

test "stream handler: a create of an existing stream is ok when it asks for the same settings, refused when not; an alter needs the stream" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var ob: [32]u8 = undefined;
    var b = OptionsBuilder.init(&ob);
    try b.addU64(.retention_count, 5);
    var pc: [4]u8 = undefined;
    std.mem.writeInt(u32, &pc, 4, .little);
    try testing.expectEqual(CommandResult.ok, handler.handleCommand(makeRequest(.stream_create, "c", &pc, b.getOptions())));
    // A retry.
    try testing.expectEqual(CommandResult.ok, handler.handleCommand(makeRequest(.stream_create, "c", &pc, b.getOptions())));
    // Other settings.
    switch (handler.handleCommand(makeRequest(.stream_create, "c", "", b.getOptions()))) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.already_exists, e.code),
        else => return error.TestUnexpectedResult,
    }
    switch (handler.handleCommand(makeRequest(.stream_alter, "nope", "", b.getOptions()))) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.not_found, e.code),
        else => return error.TestUnexpectedResult,
    }
    var ob2: [32]u8 = undefined;
    var b2 = OptionsBuilder.init(&ob2);
    try b2.addU64(.retention_count, 9);
    try testing.expectEqual(CommandResult.ok, handler.handleCommand(makeRequest(.stream_alter, "c", "", b2.getOptions())));
    const meta = metaFor(&handler, "c").?;
    try testing.expectEqual(@as(u32, 4), meta.partition_count);
    try testing.expectEqual(@as(u64, 9), meta.retention_count);
}

test "stream handler: stream_config entries give every replica a stream's settings, and replay rebuilds them" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    const ns: router.NamespaceId = .default;
    var value: [StreamHandler.StreamConfig.SIZE]u8 = undefined;
    var pbuf: [128]u8 = undefined;
    (StreamHandler.StreamConfig{ .op = .create, .partition_count = 3, .retention_age_s = 60, .retention_count = 0 }).encode(&value);
    const create = entry_mod.buildCommandEntry(.stream_config, entry_mod.Flags.NONE, 1, 1, 0, ns, "r", &value, &pbuf).?;
    var out = try applyLogged(&handler, &create);
    try testing.expect(out.refused() == null);
    const meta = metaFor(&handler, "r").?;
    try testing.expectEqual(@as(u32, 3), meta.partition_count);
    try testing.expectEqual(@as(u64, 60), meta.retention_age_s);

    // A second create asking for other settings is refused the same way
    // everywhere, and changes nothing.
    (StreamHandler.StreamConfig{ .op = .create, .partition_count = 1, .retention_age_s = 0, .retention_count = 0 }).encode(&value);
    var pbuf2: [128]u8 = undefined;
    const again = entry_mod.buildCommandEntry(.stream_config, entry_mod.Flags.NONE, 1, 2, 0, ns, "r", &value, &pbuf2).?;
    out = try applyLogged(&handler, &again);
    try testing.expectEqual(proto.StatusCode.conflict, out.refused().?.status);
    try testing.expectEqual(@as(u32, 3), metaFor(&handler, "r").?.partition_count);

    // An alter merges: what it names changes, the rest stays.
    (StreamHandler.StreamConfig{ .op = .alter, .partition_count = 0, .retention_age_s = 0, .retention_count = 7 }).encode(&value);
    var pbuf3: [128]u8 = undefined;
    const alter = entry_mod.buildCommandEntry(.stream_config, entry_mod.Flags.NONE, 1, 3, 0, ns, "r", &value, &pbuf3).?;
    out = try applyLogged(&handler, &alter);
    try testing.expect(out.refused() == null);
    const merged = metaFor(&handler, "r").?;
    try testing.expectEqual(@as(u32, 3), merged.partition_count);
    try testing.expectEqual(@as(u64, 60), merged.retention_age_s);
    try testing.expectEqual(@as(u64, 7), merged.retention_count);
}

test "stream handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    StreamHandler.register(&dispatcher);

    // Stream opcodes
    try testing.expect(dispatcher.hasHandler(OpCode.stream_append));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_read));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_trim));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_info));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_list));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_create));

    // Consumer group opcodes
    try testing.expect(dispatcher.hasHandler(OpCode.stream_group_create));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_group_read));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_group_ack));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_group_nack));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_group_delete));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_group_info));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_group_pending));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_group_touch));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_alter));

    try testing.expect(dispatcher.hasHandler(OpCode.stream_delete));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_group_list));
    try testing.expect(dispatcher.hasHandler(OpCode.stream_group_members));

    try testing.expectEqual(@as(u16, 26), dispatcher.handler_count);
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

test "stream handler: a read of a stream that doesn't exist is not found; one that exists with nothing to read is empty" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    const Read = struct {
        fn count(h: *StreamHandler, name: []const u8) !?u32 {
            const result = h.handleCommand(makeRequest(.stream_read, name, "", ""));
            defer h.freeResult(result);
            switch (result) {
                .stream_messages => |m| return std.mem.readInt(u32, m.data[0..4], .little),
                .err => |e| {
                    try testing.expectEqual(CommandResult.ErrorCode.not_found, e.code);
                    try testing.expectEqualStrings("stream not found", e.message);
                    // Its reason too, by which a client tells it from a refusal.
                    try testing.expectEqual(proto.Reason.not_found, e.refusal().reason);
                    return null;
                },
                else => return error.TestUnexpectedResult,
            }
        }
    };
    try testing.expectEqual(@as(?u32, null), try Read.count(&handler, "never"));
    // Created, nothing appended: empty.
    _ = handler.handleCommand(makeRequest(.stream_create, "empty", "", ""));
    try testing.expectEqual(@as(?u32, 0), try Read.count(&handler, "empty"));
    // Appended, then read past its last record: empty.
    var vb: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "one", makeBatchValue(&vb, "a"), ""));
    try testing.expectEqual(@as(?u32, 1), try Read.count(&handler, "one"));
    var ob: [32]u8 = undefined;
    var tail = OptionsBuilder.init(&ob);
    try tail.addFlag(.stream_tail);
    const past = handler.handleCommand(makeRequest(.stream_read, "one", "", tail.getOptions()));
    defer handler.freeResult(past);
    try testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, past.stream_messages.data[0..4], .little));
    // Deleted: not found again.
    var fb: [8]u8 = undefined;
    var force = OptionsBuilder.init(&fb);
    try force.addFlag(.force);
    _ = handler.handleCommand(makeRequest(.stream_delete, "one", "", force.getOptions()));
    try testing.expectEqual(@as(?u32, null), try Read.count(&handler, "one"));
}

fn makeNRecordBatch(buf: []u8, n: u32, tag: u8) []const u8 {
    std.mem.writeInt(u32, buf[0..4], n, .little);
    var pos: usize = 4;
    for (0..n) |_| {
        std.mem.writeInt(u32, buf[pos..][0..4], 1, .little);
        buf[pos + 4] = tag;
        std.mem.writeInt(u16, buf[pos + 5 ..][0..2], 0, .little);
        pos += 7;
    }
    return buf[0..pos];
}

test "stream handler: append refuses a batch over the limit or malformed, by name" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var big_buf: [4 + (stream_mod.MAX_BATCH_RECORDS + 1) * 7]u8 = undefined;
    const big = makeNRecordBatch(&big_buf, stream_mod.MAX_BATCH_RECORDS + 1, 'x');
    switch (handler.handleCommand(makeRequest(.stream_append, "s1", big, ""))) {
        .err => |e| try testing.expectEqualStrings("stream append: a batch of 1001 records is over the limit of 1000", e.message),
        else => return error.TestUnexpectedResult,
    }
    for ([_][]const u8{ "", "raw payload" }) |v| switch (handler.handleCommand(makeRequest(.stream_append, "s1", v, ""))) {
        .err => |e| try testing.expectEqualStrings("stream append: malformed batch", e.message),
        else => return error.TestUnexpectedResult,
    };
    try testing.expectEqual(@as(u64, 0), handler.stream.streamLogicalCount(stream_mod.streamKey(.default, "s1")));
}

test "stream handler: a read returns whole appends and its cursor passes them" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 1 << 20, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var bufs: [3][4 + 400 * 7]u8 = undefined;
    for (0..3) |i| _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeNRecordBatch(&bufs[i], 400, @intCast('a' + i)), ""));

    // A budget of 500 records takes one 400-record append whole, not 500
    // records split across two; the cursor is that append's last record.
    var after: ?StreamID = null;
    var seen: [3]u8 = undefined;
    for (0..3) |round| {
        var ob: [64]u8 = undefined;
        var b = OptionsBuilder.init(&ob);
        try b.addU32(.count, 500);
        if (after) |a| try b.addStreamId(.stream_start, a.timestamp_ms, a.sequence);
        const result = handler.handleCommand(makeRequest(.stream_read, "s1", "", b.getOptions()));
        switch (result) {
            .stream_messages => |m| {
                defer handler.freeResult(result);
                try testing.expectEqual(@as(u32, 400), std.mem.readInt(u32, m.data[0..4], .little));
                const first_seq = std.mem.readInt(u64, m.data[4..12], .little);
                try testing.expectEqual(first_seq + 399, m.next_sequence);
                // [seq:u64][ts:i64][tier:u8][partition:u32][key_present=0:u8][len:u32][payload]
                try testing.expectEqual(@as(u8, 0), m.data[4 + 8 + 8 + 1 + 4]);
                seen[round] = m.data[4 + 8 + 8 + 1 + 4 + 1 + 4];
                after = .{ .timestamp_ms = m.next_timestamp_ms, .sequence = m.next_sequence };
            },
            else => return error.TestUnexpectedResult,
        }
    }
    try testing.expectEqualStrings("abc", &seen);
}

test "stream handler: the processing source resumes after whole appends" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 1 << 20, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var bufs: [3][4 + 400 * 7]u8 = undefined;
    for (0..3) |i| _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeNRecordBatch(&bufs[i], 400, @intCast('a' + i)), ""));

    var after = StreamID.MIN;
    for ("abc") |tag| {
        const got = handler.readPayloadsForStream("s1", "default", after, 500);
        defer allocator.free(got.payloads);
        defer allocator.free(got.ids);
        try testing.expectEqual(@as(usize, 400), got.payloads.len);
        for (got.payloads) |p| try testing.expectEqual(tag, p[0]);
        // The cursor is the append's last record, not its first.
        try testing.expectEqual(got.ids[0].sequence + 399, got.last_id.sequence);
        after = got.last_id;
    }
    const done = handler.readPayloadsForStream("s1", "default", after, 500);
    try testing.expectEqual(@as(usize, 0), done.payloads.len);
}

test "stream handler: one maximal append's answer is within its bound" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 1 << 20, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // 1000 empty records with 14 empty headers each: the shape whose answer
    // grows most over its stored size (each header's lengths double).
    const n = stream_mod.MAX_BATCH_RECORDS;
    const per = 4 + 2 + 14 * 4;
    const blob = try allocator.alloc(u8, 4 + n * per);
    defer allocator.free(blob);
    @memset(blob, 0);
    std.mem.writeInt(u32, blob[0..4], n, .little);
    for (0..n) |i| std.mem.writeInt(u16, blob[4 + i * per + 4 ..][0..2], 14, .little);
    switch (handler.handleCommand(makeRequest(.stream_append, "s1", blob, ""))) {
        .stream_append_ok => {},
        else => return error.TestUnexpectedResult,
    }

    const h = stream_mod.streamKey(.default, "s1");
    var recs: [1]StreamRecord = undefined;
    try testing.expectEqual(@as(usize, 1), handler.stream.readStreamAfter(h, StreamID.MIN, null, &recs));
    const data = try handler.serializeStreamRecordsWithPayloads(&recs, StreamID.MIN, null);
    defer allocator.free(data);
    try testing.expectEqual(@as(u32, n), std.mem.readInt(u32, data[0..4], .little));
    try testing.expect(data.len <= 4 + stream_mod.answerBound(recs[0]));
}

test "stream handler: a read steps past an append it can't read" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 1 << 20, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // An append the stream holds but whose payload is in no tier.
    const lost = try appendRecord(&handler, .default, "s1", 999_999, 1_700_000_000_000, 1);
    var vb: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vb, "ok"), ""));

    // A one-record read meets only the lost append first; it steps past it
    // rather than answering empty with the cursor left where it was.
    var ob: [32]u8 = undefined;
    var b = OptionsBuilder.init(&ob);
    try b.addU32(.count, 1);
    const result = handler.handleCommand(makeRequest(.stream_read, "s1", "", b.getOptions()));
    switch (result) {
        .stream_messages => |m| {
            defer handler.freeResult(result);
            try testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, m.data[0..4], .little));
            try testing.expect(std.mem.endsWith(u8, m.data, "ok\x00\x00\x00\x00"));
            try testing.expect(StreamID.greaterThan(.{ .timestamp_ms = m.next_timestamp_ms, .sequence = m.next_sequence }, lost));
        },
        else => return error.TestUnexpectedResult,
    }
    try testing.expectEqual(@as(u64, 1), handler.skipped_appends);

    // Read again from the start: the same lost append isn't counted twice.
    const again = handler.handleCommand(makeRequest(.stream_read, "s1", "", b.getOptions()));
    handler.freeResult(again);
    try testing.expectEqual(@as(u64, 1), handler.skipped_appends);
}

test "stream handler: a processing read skips and counts an append in no tier, and its cursor passes it" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 1 << 20, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    const lost = try appendRecord(&handler, .default, "s1", 999_999, 1_700_000_000_000, 1);

    const read = handler.readPayloadsForStream("s1", "default", StreamID.MIN, 10);
    defer allocator.free(read.payloads);
    defer allocator.free(read.ids);
    try testing.expectEqual(@as(usize, 0), read.payloads.len);
    // Passed, or a source would read it again forever.
    try testing.expect(read.last_id.eql(lost));
    try testing.expectEqual(@as(u64, 1), handler.skipped_appends);
}

test "stream handler: read honors count" {
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

    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addU32(.count, 2);
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

test "stream handler: partition_index survives restart/replay" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // Append one record to each of three distinct user partitions.
    const parts = [_]struct { payload: []const u8, partition: u32 }{
        .{ .payload = "a", .partition = 0 },
        .{ .payload = "b", .partition = 1 },
        .{ .payload = "c", .partition = 2 },
    };
    for (parts) |p| {
        var vbuf: [64]u8 = undefined;
        var obuf: [16]u8 = undefined;
        var ob = OptionsBuilder.init(&obuf);
        try ob.addU32(.partition, p.partition);
        _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vbuf, p.payload), ob.getOptions()));
    }

    // Pre-restart: each partition filter isolates exactly its one record.
    try testing.expectEqual(@as(u32, 1), try readPartitionCount(&handler, "s1", 0));
    try testing.expectEqual(@as(u32, 1), try readPartitionCount(&handler, "s1", 1));
    try testing.expectEqual(@as(u32, 1), try readPartitionCount(&handler, "s1", 2));

    // Simulate restart: drop the shard's state, then rebuild it from the
    // persisted UAL entries — exactly the segment-replay path used on boot.
    partition.store.clear();
    var idx: u64 = 1;
    while (idx <= partition.committed_index) : (idx += 1) {
        if (partition.ual.read(idx)) |entry| {
            _ = try applyLogged(&handler, &entry);
        }
    }

    // After restart, partitions must still be isolated; if replay lost the
    // partition, every record would rebuild as partition 0 and read 3/0/0.
    try testing.expectEqual(@as(u32, 1), try readPartitionCount(&handler, "s1", 0));
    try testing.expectEqual(@as(u32, 1), try readPartitionCount(&handler, "s1", 1));
    try testing.expectEqual(@as(u32, 1), try readPartitionCount(&handler, "s1", 2));
}

test "stream handler: an append that runs out of memory fails, rather than apply unlisted" {
    var fail_at: usize = 0;
    while (true) : (fail_at += 1) {
        var fa = std.testing.FailingAllocator.init(testing.allocator, .{});
        var partition = try Partition.init(fa.allocator(), 0, 4096, 0);
        defer partition.deinit();
        partition.wireProjections();
        var handler = StreamHandler.init(fa.allocator(), &partition);
        defer handler.deinit();
        var vbuf: [64]u8 = undefined;
        _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vbuf, "a"), ""));
        const entry = partition.ual.read(1) orelse return error.EntryMissing;

        // Replayed from scratch, each allocation failed in turn.
        partition.store.clear();
        fa.fail_index = fa.alloc_index + fail_at;
        const outcome = applyLogged(&handler, &entry);
        if (outcome) |_| try testing.expect((metaFor(&handler, "s1") != null)) else |err| try testing.expectEqual(error.OutOfMemory, err);
        if (!fa.has_induced_failure) {
            _ = try outcome;
            break;
        }
    }
}

test "stream handler: delete removes one stream, leaves siblings + --force gating" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var vb1: [64]u8 = undefined;
    var vb2: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "orders", makeBatchValue(&vb1, "o1"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "shipments", makeBatchValue(&vb2, "s1"), ""));

    const ns_hash: router.NamespaceId = .default;
    const orders_hash = stream_mod.streamKey(ns_hash, "orders");
    const ship_hash = stream_mod.streamKey(ns_hash, "shipments");
    try testing.expectEqual(@as(u64, 1), handler.stream.streamLogicalCount(orders_hash));

    // Non-empty without --force → conflict, stream untouched.
    const no_force = handler.handleCommand(makeRequest(.stream_delete, "orders", "", ""));
    try testing.expect(std.meta.activeTag(no_force) == .err);
    try testing.expectEqual(@as(u64, 1), handler.stream.streamLogicalCount(orders_hash));

    // With --force → ok; orders gone, shipments intact.
    var ob: [8]u8 = undefined;
    var builder = OptionsBuilder.init(&ob);
    try builder.addFlag(.force);
    const forced = handler.handleCommand(makeRequest(.stream_delete, "orders", "", builder.getOptions()));
    try testing.expect(std.meta.activeTag(forced) == .ok);
    try testing.expectEqual(@as(usize, 0), handler.stream.streamLogicalCount(orders_hash));
    try testing.expectEqual(@as(u64, 1), handler.stream.streamLogicalCount(ship_hash));

    // Idempotent: deleting the now-missing stream succeeds.
    const again = handler.handleCommand(makeRequest(.stream_delete, "orders", "", ""));
    try testing.expect(std.meta.activeTag(again) == .ok);
}

test "stream handler: a stream_delete entry without force is refused for a stream that holds records, on replay too" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var vb: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "orders", makeBatchValue(&vb, "o1"), ""));
    const orders_hash = stream_mod.streamKey(.default, "orders");
    var pbuf: [128]u8 = undefined;
    const del = entry_mod.buildCommandEntry(.stream_delete, entry_mod.Flags.NONE, 0, partition.committed_index + 1, 1, .default, "orders", "", &pbuf).?;
    const out = try applyLogged(&handler, &del);
    const refused = out.refused().?;
    try testing.expectEqual(proto.StatusCode.conflict, refused.status);
    try testing.expectEqual(proto.Ran.no, refused.refusal.ran);
    try testing.expectEqual(@as(u64, 1), handler.stream.streamLogicalCount(orders_hash));

    // An empty stream, or none, is deleted all the same.
    var ebuf: [128]u8 = undefined;
    const none = entry_mod.buildCommandEntry(.stream_delete, entry_mod.Flags.NONE, 0, partition.committed_index + 2, 1, .default, "never", "", &ebuf).?;
    try testing.expect((try applyLogged(&handler, &none)).refused() == null);
}

test "stream handler: stream_delete replay/follower path wipes the stream" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var vb: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "orders", makeBatchValue(&vb, "o1"), ""));
    const ns_hash: router.NamespaceId = .default;
    const orders_hash = stream_mod.streamKey(ns_hash, "orders");
    try testing.expectEqual(@as(u64, 1), handler.stream.streamLogicalCount(orders_hash));

    // A replicated/replayed stream_delete entry (key = the stream's name)
    // must remove the stream — this is the exact path followers and restart use.
    var pbuf: [128]u8 = undefined;
    const del = entry_mod.buildCommandEntry(.stream_delete, entry_mod.Flags.FORCE, 0, partition.committed_index + 1, 1, .default, "orders", "", &pbuf).?;
    _ = try applyLogged(&handler, &del);
    try testing.expectEqual(@as(usize, 0), handler.stream.streamLogicalCount(orders_hash));
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

    const s1_hash = stream_mod.streamKey(.default, "s1");

    // Keep the newest 2: a dry run counts the 3 it would remove and removes none.
    var dry_buf: [32]u8 = undefined;
    var dry = OptionsBuilder.init(&dry_buf);
    try dry.addU64(.limit, 2);
    try dry.addFlag(.dry_run);
    const dry_first_seq = switch (handler.handleCommand(makeRequest(.stream_trim, "s1", "", dry.getOptions()))) {
        .stream_trimmed => |t| blk: {
            try testing.expectEqual(@as(u64, 3), t.deleted_count);
            break :blk t.first_seq;
        },
        else => return error.TestUnexpectedResult,
    };
    try testing.expectEqual(@as(u64, 5), handler.stream.streamLogicalCount(s1_hash));

    // The real trim removes the number the dry run reported.
    var real_buf: [32]u8 = undefined;
    var real = OptionsBuilder.init(&real_buf);
    try real.addU64(.limit, 2);
    switch (handler.handleCommand(makeRequest(.stream_trim, "s1", "", real.getOptions()))) {
        .stream_trimmed => |t| {
            try testing.expectEqual(@as(u64, 3), t.deleted_count);
            try testing.expectEqual(dry_first_seq, t.first_seq);
            try testing.expectEqual(handler.stream.streamFirstId(s1_hash).sequence, t.first_seq);
        },
        else => return error.TestUnexpectedResult,
    }
    try testing.expectEqual(@as(u64, 2), handler.stream.streamLogicalCount(s1_hash));
}

fn expectTrimRefused(handler: *StreamHandler, value: []const u8, options: []const u8, message: []const u8) !void {
    switch (handler.handleCommand(makeRequest(.stream_trim, "s1", value, options))) {
        .err => |e| try testing.expectEqualStrings(message, e.message),
        else => return error.TestUnexpectedResult,
    }
}

test "stream handler: trim refuses what it would otherwise ignore" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var vba: [64]u8 = undefined;
    var vbb: [64]u8 = undefined;
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vba, "a"), ""));
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", makeBatchValue(&vbb, "b"), ""));

    // Each request carries a valid bound, so only the named cause can refuse it.
    var ok_buf: [32]u8 = undefined;
    var ok = OptionsBuilder.init(&ok_buf);
    try ok.addU64(.limit, 1);
    try expectTrimRefused(&handler, "3", ok.getOptions(), "stream trim takes no value; give before, maxlen or maxage");

    inline for (.{
        .{ proto.OptionTag.max_bytes, "stream trim: max_bytes is not supported; give one of before, maxlen or maxage" },
        .{ proto.OptionTag.stream_end, "stream trim: stream_end is not a trim bound; give one of before, maxlen or maxage" },
        .{ proto.OptionTag.retention_count, "stream trim: retention_count is not a trim bound; send limit (maxlen)" },
        .{ proto.OptionTag.retention_age, "stream trim: retention_age is not a trim bound; send max_age_seconds (maxage)" },
        .{ proto.OptionTag.retention_bytes, "stream trim: retention_bytes is not supported; give one of before, maxlen or maxage" },
    }) |c| {
        var ob: [32]u8 = undefined;
        var b = OptionsBuilder.init(&ob);
        try b.addU64(.limit, 1);
        try b.addU64(c[0], 10);
        try expectTrimRefused(&handler, "", b.getOptions(), c[1]);
    }

    var two_buf: [32]u8 = undefined;
    var two = OptionsBuilder.init(&two_buf);
    try two.addU64(.limit, 1);
    try two.addU64(.max_age_seconds, 60);
    try expectTrimRefused(&handler, "", two.getOptions(), "stream trim: give only one of before, maxlen or maxage");

    try expectTrimRefused(&handler, "", "", "stream trim: give one of before, maxlen or maxage");

    var zl_buf: [16]u8 = undefined;
    var zl = OptionsBuilder.init(&zl_buf);
    try zl.addU64(.limit, 0);
    try expectTrimRefused(&handler, "", zl.getOptions(), "stream trim: maxlen must be > 0");

    var za_buf: [16]u8 = undefined;
    var za = OptionsBuilder.init(&za_buf);
    try za.addU64(.max_age_seconds, 0);
    try expectTrimRefused(&handler, "", za.getOptions(), "stream trim: maxage must be > 0");

    // Past 100 years an age is a mistake, refused rather than saturated.
    var big_buf: [16]u8 = undefined;
    var big = OptionsBuilder.init(&big_buf);
    try big.addU64(.max_age_seconds, time_units.MAX_AGE_MS / std.time.ms_per_s + 1);
    try expectTrimRefused(&handler, "", big.getOptions(), "stream trim: maxage is over 100 years");

    // Nothing was trimmed by any of them.
    try testing.expectEqual(@as(u64, 2), handler.stream.streamLogicalCount(stream_mod.streamKey(.default, "s1")));
}

test "stream handler: a trim by age cuts at its entry's stamp, not the host's clock" {
    const allocator = testing.allocator;
    const host_ms: u64 = @intCast(@import("stdx").time.milliTimestamp());
    // One stamp far behind the host's clock and one a day ahead: a trim
    // cut by the host's clock removes all three records or none.
    for ([_]u64{ 1_000_000, host_ms + std.time.ms_per_day }) |stamp_ms| {
        var partition = try Partition.init(allocator, 0, 4096, 0);
        defer partition.deinit();
        partition.wireProjections();
        var handler = StreamHandler.init(allocator, &partition);
        defer handler.deinit();

        const h = stream_mod.streamKey(.default, "aged");
        // 3 s old exactly is not older than 3 s: it stays.
        for ([_]u64{ 10_000, 5_000, 3_000, 1_000 }, 1..) |age, i| {
            _ = try appendRecord(&handler, .default, "aged", i, stamp_ms - age, 1);
        }
        var key_buf: [TrimBound.MAX_SIZE]u8 = undefined;
        // The entry carries the namespace; its value, the rest of the key.
        const val = h[4..12];
        var pbuf: [128]u8 = undefined;
        // Older than 3 s at the stamp: the 10 s and 5 s old records.
        const trim = entry_mod.buildCommandEntry(.stream_trim, entry_mod.Flags.NONE, 0, 10, stamp_ms * std.time.ns_per_ms, .default, (TrimBound{ .age_ms = 3_000 }).encode(&key_buf), val, &pbuf).?;
        var trimmed_out = try applyLogged(&handler, &trim);
        var tr = trimmed_out.reader();
        try testing.expectEqual(@as(?u64, 2), tr.int(u64));
        try testing.expectEqual(@as(u64, 2), handler.stream.streamLogicalCount(h));

        // An age past the stamp trims nothing, never everything.
        const past = entry_mod.buildCommandEntry(.stream_trim, entry_mod.Flags.NONE, 0, 11, stamp_ms * std.time.ns_per_ms, .default, (TrimBound{ .age_ms = stamp_ms + 1 }).encode(&key_buf), val, &pbuf).?;
        trimmed_out = try applyLogged(&handler, &past);
        tr = trimmed_out.reader();
        try testing.expectEqual(@as(?u64, 0), tr.int(u64));
        try testing.expectEqual(@as(u64, 2), handler.stream.streamLogicalCount(h));
    }
}

test "stream handler: a trim applies in its entry's namespace alone, though the same name is a stream in another" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    const ours = stream_mod.streamKey(.of(16), "same");
    const theirs = stream_mod.streamKey(.of(17), "same");
    for (1..4) |i| {
        _ = try appendRecord(&handler, .of(16), "same", 2 * i - 1, 1_000 + i, 1);
        _ = try appendRecord(&handler, .of(17), "same", 2 * i, 1_000 + i, 1);
    }
    var key_buf: [TrimBound.MAX_SIZE]u8 = undefined;
    const t = StreamHandler.trimCommand(&ours, .{ .id = StreamID.MAX }, &key_buf);
    var pbuf: [128]u8 = undefined;
    const trim = entry_mod.buildCommandEntry(.stream_trim, entry_mod.Flags.NONE, 0, 7, 2_000 * std.time.ns_per_ms, t.ns, t.key, t.value, &pbuf).?;
    var out = try applyLogged(&handler, &trim);
    var r = out.reader();
    try testing.expectEqual(@as(?u64, 3), r.int(u64));
    try testing.expectEqual(@as(u64, 0), handler.stream.streamLogicalCount(ours));
    try testing.expectEqual(@as(u64, 3), handler.stream.streamLogicalCount(theirs));
}

test "stream handler: --maxage counts in seconds" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    const h = stream_mod.streamKey(.default, "s1");
    const now_ms: u64 = @intCast(@import("stdx").time.milliTimestamp());
    _ = try appendRecord(&handler, .default, "s1", 1, now_ms - 10_000, 1);
    _ = try appendRecord(&handler, .default, "s1", 2, now_ms - 1_000, 1);
    // Older than 5 seconds: the 10 s old record, not the 1 s old one.
    var ob: [16]u8 = undefined;
    var b = OptionsBuilder.init(&ob);
    try b.addU64(.max_age_seconds, 5);
    switch (handler.handleCommand(makeRequest(.stream_trim, "s1", "", b.getOptions()))) {
        .stream_trimmed => |t| try testing.expectEqual(@as(u64, 1), t.deleted_count),
        else => return error.TestUnexpectedResult,
    }
    try testing.expectEqual(@as(u64, 1), handler.stream.streamLogicalCount(h));
}

test "stream handler: a trim bound round-trips, and a key of any other shape is refused" {
    var buf: [TrimBound.MAX_SIZE]u8 = undefined;
    const id: TrimBound = .{ .id = .{ .timestamp_ms = 7, .sequence = 9 } };
    try testing.expectEqual(id, TrimBound.decode(id.encode(&buf)).?);
    const age: TrimBound = .{ .age_ms = 60_000 };
    try testing.expectEqual(age, TrimBound.decode(age.encode(&buf)).?);
    try testing.expect(TrimBound.decode(&@as([16]u8, @splat(0))) == null);
    try testing.expect(TrimBound.decode(&([_]u8{2} ++ @as([8]u8, @splat(0)))) == null);
}

test "stream handler: a retention age past 100 years is refused at create and alter" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // Alter needs a stream to alter.
    try testing.expectEqual(CommandResult.ok, handler.handleCommand(makeRequest(.stream_create, "aged", "", "")));
    var ob: [16]u8 = undefined;
    var b = OptionsBuilder.init(&ob);
    try b.addU64(.retention_age, time_units.MAX_AGE_MS / std.time.ms_per_s + 1);
    for ([_]OpCode{ .stream_create, .stream_alter }) |op| {
        switch (handler.handleCommand(makeRequest(op, "aged", "", b.getOptions()))) {
            .err => |e| try testing.expectEqualStrings("stream retention age is over 100 years", e.message),
            else => return error.TestUnexpectedResult,
        }
    }
}

test "stream handler: --maxlen cutting inside the first batch trims nothing" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();

    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    // One 3-record batch: keeping 1 would cut inside it.
    const batch = [_]u8{ 3, 0, 0, 0, 1, 0, 0, 0, 'a', 0, 0, 1, 0, 0, 0, 'b', 0, 0, 1, 0, 0, 0, 'c', 0, 0 };
    _ = handler.handleCommand(makeRequest(.stream_append, "s1", &batch, ""));
    const h = stream_mod.streamKey(.default, "s1");
    try testing.expectEqual(@as(u64, 3), handler.stream.streamLogicalCount(h));

    for ([_]bool{ true, false }) |dry| {
        var ob: [32]u8 = undefined;
        var b = OptionsBuilder.init(&ob);
        try b.addU64(.limit, 1);
        if (dry) try b.addFlag(.dry_run);
        switch (handler.handleCommand(makeRequest(.stream_trim, "s1", "", b.getOptions()))) {
            .stream_trimmed => |t| try testing.expectEqual(@as(u64, 0), t.deleted_count),
            else => return error.TestUnexpectedResult,
        }
    }
    try testing.expectEqual(@as(u64, 3), handler.stream.streamLogicalCount(h));
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

test "stream handler: byte retention is refused, and a refused create registers nothing" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var ob: [32]u8 = undefined;
    var b = OptionsBuilder.init(&ob);
    try b.addU64(.retention_bytes, 1024);
    switch (handler.handleCommand(makeRequest(.stream_create, "sized", "", b.getOptions()))) {
        .err => |e| try testing.expectEqualStrings("stream retention by bytes is not supported; use retention_age or retention_count", e.message),
        else => return error.TestUnexpectedResult,
    }
    try testing.expect(metaFor(&handler, "sized") == null);

    _ = handler.handleCommand(makeRequest(.stream_create, "sized", "", ""));
    switch (handler.handleCommand(makeRequest(.stream_alter, "sized", "", b.getOptions()))) {
        .err => |e| try testing.expectEqualStrings("stream retention by bytes is not supported; use retention_age or retention_count", e.message),
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

    // No key: no hash; the request runs on group 0
    const req_empty = makeRequest(.stream_append, "", "", "");
    try testing.expectEqual(@as(?u64, null), StreamHandler.preRouteByStream(req_empty));

    // Same stream, different namespace → different hash (namespace isolation)
    var req_ns = makeRequest(.stream_append, "stream-a", "", "");
    req_ns.namespace = "other";
    req_ns.ns_id = .of(16);
    try testing.expect(StreamHandler.preRouteByStream(req1) != StreamHandler.preRouteByStream(req_ns));
}

test "stream handler: a stream name is refused when empty, holding a NUL, or too long for a key; a list's key is a filter" {
    const long: [ns_keys.MAX_QUALIFIED_KEY + 1]u8 = @splat('s');
    try testing.expectEqualStrings("stream name is required", StreamHandler.nameRefusal(.stream_append, makeRequest(.stream_append, "", "", "")).?);
    try testing.expectEqualStrings("stream name must not contain NUL", StreamHandler.nameRefusal(.stream_read, makeRequest(.stream_read, "a\x00b", "", "")).?);
    try testing.expectEqualStrings("stream name too long", StreamHandler.nameRefusal(.stream_append, makeRequest(.stream_append, &long, "", "")).?);
    try testing.expect(StreamHandler.nameRefusal(.stream_append, makeRequest(.stream_append, long[0..ns_keys.MAX_QUALIFIED_KEY], "", "")) == null);
    try testing.expect(StreamHandler.nameRefusal(.stream_list, makeRequest(.stream_list, "a\x00b", "", "")) == null);
}

test "stream handler: a stream's existence and settings live in the contract's state: a created stream with no records, and its retention, read from its families alone" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var ob: [32]u8 = undefined;
    var b = OptionsBuilder.init(&ob);
    try b.addU64(.retention_count, 3);
    var pc: [4]u8 = undefined;
    std.mem.writeInt(u32, &pc, 2, .little);
    try testing.expectEqual(CommandResult.ok, handler.handleCommand(makeRequest(.stream_create, "empty", &pc, b.getOptions())));
    // What a snapshot encodes is the contract's families: the stream is in them.
    try testing.expect(partition.store.family(stream_machine.Metas.ID).count == 1);
    try testing.expect(partition.store.family(stream_machine.Retained.ID).count == 1);

    // Read from those families alone: the stream exists, empty, with its
    // settings.
    switch (handler.handleCommand(makeRequest(.stream_info, "empty", "", ""))) {
        .stream_info => |i| {
            try testing.expectEqual(@as(u64, 0), i.count);
            try testing.expectEqual(@as(u32, 2), i.partition_count);
            try testing.expectEqual(@as(u64, 3), i.retention_count);
        },
        else => return error.TestUnexpectedResult,
    }
    const names = StreamHandler.localScanStreams(@ptrCast(&handler), .{ .id = .default, .name = "default" }, "", null, 10);
    try testing.expectEqual(@as(usize, 1), names.items.len);
    try testing.expectEqualStrings("empty", names.items[0]);
}

test "stream handler: a list skips a tombstone, a deleted stream its groups still count" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();
    var pc: [4]u8 = undefined;
    std.mem.writeInt(u32, &pc, 1, .little);
    for ([_][]const u8{ "kept", "gone" }) |name| try testing.expectEqual(CommandResult.ok, handler.handleCommand(makeRequest(.stream_create, name, &pc, "")));
    // "gone" as a delete leaves it while groups count it.
    const sk = stream_mod.streamKey(.default, "gone");
    const node = partition.store.find(stream_machine.Metas.ID, &sk).?;
    var m = (try stream_machine.Metas.Value.decode(@import("../contract/store.zig").Record.of(node).value)).v;
    m.exists = false;
    const bytes = try partition.store.allocator.alloc(u8, try stream_machine.Meta.encodedSize(m));
    _ = try stream_machine.Meta.encodeInto(bytes, m);
    partition.store.replace(stream_machine.Metas.ID, node, bytes);
    const names = StreamHandler.localScanStreams(@ptrCast(&handler), .{ .id = .default, .name = "default" }, "", null, 10);
    try testing.expectEqual(@as(usize, 1), names.items.len);
    try testing.expectEqualStrings("kept", names.items[0]);
}

test "stream handler: an alter drops a retention bound it clears and leaves the other; a create can't clear, nor an alter set and clear one bound" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var ob: [32]u8 = undefined;
    var b = OptionsBuilder.init(&ob);
    try b.addU64(.retention_count, 5);
    try b.addU64(.retention_age, 3600);
    try testing.expectEqual(CommandResult.ok, handler.handleCommand(makeRequest(.stream_create, "c", "", b.getOptions())));

    var cb: [8]u8 = undefined;
    var clear = OptionsBuilder.init(&cb);
    try clear.addFlag(.retention_count_clear);
    try testing.expectEqual(CommandResult.ok, handler.handleCommand(makeRequest(.stream_alter, "c", "", clear.getOptions())));
    const m = stream_machine.metaOf(handler.readView(), .default, "c").?;
    try testing.expectEqual(@as(u64, 0), m.retention_count);
    try testing.expectEqual(@as(u64, 3600), m.retention_age_s);

    var bb: [32]u8 = undefined;
    var both = OptionsBuilder.init(&bb);
    try both.addU64(.retention_age, 60);
    try both.addFlag(.retention_age_clear);
    for ([_]struct { op: OpCode, opts: []const u8 }{ .{ .op = .stream_alter, .opts = both.getOptions() }, .{ .op = .stream_create, .opts = clear.getOptions() } }) |c| {
        switch (handler.handleCommand(makeRequest(c.op, "c", "", c.opts))) {
            .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
            else => return error.TestUnexpectedResult,
        }
    }
    try testing.expectEqual(@as(u64, 3600), stream_machine.metaOf(handler.readView(), .default, "c").?.retention_age_s);
}

test "stream handler: a count bound the size of a clear is refused on a create and an alter, never taken as one" {
    const allocator = testing.allocator;
    var partition = try Partition.init(allocator, 0, 4096, 0);
    defer partition.deinit();
    partition.wireProjections();
    var handler = StreamHandler.init(allocator, &partition);
    defer handler.deinit();

    var ob: [32]u8 = undefined;
    var keep = OptionsBuilder.init(&ob);
    try keep.addU64(.retention_count, 5);
    try testing.expectEqual(CommandResult.ok, handler.handleCommand(makeRequest(.stream_create, "c", "", keep.getOptions())));

    var hb: [32]u8 = undefined;
    var huge = OptionsBuilder.init(&hb);
    try huge.addU64(.retention_count, StreamHandler.StreamConfig.CLEAR);
    for ([_]OpCode{ .stream_create, .stream_alter }) |op| {
        switch (handler.handleCommand(makeRequest(op, "c", "", huge.getOptions()))) {
            .err => |e| {
                try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code);
                try testing.expectEqualStrings("retention bound too large", e.message);
            },
            else => return error.TestUnexpectedResult,
        }
    }
    try testing.expectEqual(@as(u64, 5), stream_machine.metaOf(handler.readView(), .default, "c").?.retention_count);
}
