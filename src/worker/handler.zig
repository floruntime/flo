//! Worker Handler — tracks physical worker processes and their health.
//!
//! This is the **Worker Registry** — separate from action task dispatch.
//! It tracks all worker types (action workers, stream workers) with
//! unified health monitoring, heartbeats, and listing.
//!
//! ## Opcode Range
//!
//!   Commands:   0x70–0x74  (register, heartbeat, deregister, list, info)
//!   Responses:  0x75–0x77
//!
//! ## Worker Types
//!
//!   - `action` (0): Processes action tasks via action_await/complete/fail
//!   - `stream` (1): Processes stream records via consumer groups
//!
//! ## Design
//!
//! Workers self-register on connect and send periodic heartbeats.
//! The registry tracks health state and exposes it for dashboards.
//! Worker deregistration happens explicitly or via heartbeat timeout.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const router = @import("../node/router.zig");

const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;

pub const WorkerType = enum(u8) {
    action = 0,
    stream = 1,
};

pub const WorkerStatus = enum(u8) {
    active = 0,
    idle = 1,
    draining = 2,
    unhealthy = 3,
};

pub const ProcessKind = enum(u8) {
    action = 0,
    stream_consumer = 1,
};

/// A single process running on a worker (an action handler or stream consumer).
pub const ProcessInfo = struct {
    name_owned: []const u8,
    kind: ProcessKind,
    run_count: u64 = 0,
    fail_count: u64 = 0,
    last_run_at_ms: i64 = 0,
};

pub const WorkerRecord = struct {
    id_owned: []const u8,
    worker_type: WorkerType,
    status: WorkerStatus,
    /// JSON metadata: labels, extra user data
    metadata_owned: ?[]const u8 = null,
    /// Machine/host identifier — groups workers on the same machine
    machine_id_owned: ?[]const u8 = null,
    /// Per-process tracking (actions handled, streams consumed)
    processes: std.ArrayList(ProcessInfo),
    tasks_completed: u64 = 0,
    tasks_failed: u64 = 0,
    current_load: u32 = 0,
    max_concurrency: u32 = 10,
    registered_at_ms: i64,
    last_heartbeat_ms: i64,
};

pub const WorkerHandler = struct {
    allocator: Allocator,
    workers: std.StringHashMap(WorkerRecord),

    const MAX_WORKERS: usize = 10_000;
    const HEARTBEAT_TIMEOUT_MS: i64 = 90_000; // 90s — unhealthy if no heartbeat

    pub fn init(allocator: Allocator) WorkerHandler {
        return .{
            .allocator = allocator,
            .workers = std.StringHashMap(WorkerRecord).init(allocator),
        };
    }

    pub fn deinit(self: *WorkerHandler) void {
        var it = self.workers.iterator();
        while (it.next()) |entry| {
            self.freeWorkerRecord(entry.value_ptr);
        }
        self.workers.deinit();
    }

    fn freeWorkerRecord(self: *WorkerHandler, w: *WorkerRecord) void {
        for (w.processes.items) |p| {
            self.allocator.free(p.name_owned);
        }
        w.processes.deinit(self.allocator);
        self.allocator.free(w.id_owned);
        if (w.metadata_owned) |m| self.allocator.free(m);
        if (w.machine_id_owned) |mid| self.allocator.free(mid);
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.worker_register, dispatchWorkerCmd, preRouteByWorker);
        dispatcher.registerWithRoute(.worker_heartbeat, dispatchWorkerCmd, preRouteByWorker);
        dispatcher.registerWithRoute(.worker_deregister, dispatchWorkerCmd, preRouteByWorker);
        dispatcher.registerWithRoute(.worker_drain, dispatchWorkerCmd, preRouteByWorker);
        dispatcher.registerWalk(.worker_list, dispatchWorkerCmd, localScanWorkers);
        dispatcher.registerWithRoute(.worker_info, dispatchWorkerCmd, preRouteByWorker);
    }

    fn preRouteByWorker(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    fn dispatchWorkerCmd(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const op: OpCode = @enumFromInt(req.header.op_code);

        switch (op) {
            .worker_register => {
                shard.worker_handler.handleRegister(req);
                shard.sendOkResponse(conn, req.header.request_id, "");
            },
            .worker_heartbeat => {
                if (shard.worker_handler.handleHeartbeat(req)) |status| {
                    var status_buf: [1]u8 = .{@intFromEnum(status)};
                    shard.sendOkResponse(conn, req.header.request_id, &status_buf);
                } else {
                    shard.sendErrorResponse(conn, req.header.request_id, .not_found, "worker not found");
                }
            },
            .worker_deregister => {
                shard.worker_handler.handleDeregister(req);
                shard.sendOkResponse(conn, req.header.request_id, "");
            },
            .worker_drain => {
                if (shard.worker_handler.handleDrain(req)) {
                    shard.sendOkResponse(conn, req.header.request_id, "");
                } else {
                    shard.sendErrorResponse(conn, req.header.request_id, .not_found, "worker not found");
                }
            },
            .worker_list => {
                const data = shard.worker_handler.serializeWorkerList() catch {
                    shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "serialization failed");
                    return;
                };
                defer shard.worker_handler.allocator.free(data);
                shard.sendOkResponse(conn, req.header.request_id, data);
            },
            .worker_info => {
                const data = shard.worker_handler.serializeWorkerInfo(req.key) catch |err| switch (err) {
                    error.NotFound => {
                        shard.sendErrorResponse(conn, req.header.request_id, .not_found, "worker not found");
                        return;
                    },
                    else => {
                        shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "serialization failed");
                        return;
                    },
                };
                defer shard.worker_handler.allocator.free(data);
                shard.sendOkResponse(conn, req.header.request_id, data);
            },
            else => {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "unknown worker opcode");
            },
        }
    }

    // ── Command Handlers ────────────────────────────────────────────────

    /// Register a worker. key = worker_id.
    /// value = [type:u8][max_concurrency:u32][process_count:u16]
    ///   ([name_len:u16][name][kind:u8])*
    ///   [has_metadata:u8][metadata_len:u16][metadata]?
    fn handleRegister(self: *WorkerHandler, req: Request) void {
        if (req.key.len == 0) return;
        if (self.workers.count() >= MAX_WORKERS and !self.workers.contains(req.key)) return;

        const value = req.value;
        var offset: usize = 0;

        // Parse worker type
        const worker_type: WorkerType = if (value.len > 0)
            @enumFromInt(value[0])
        else
            .action;
        offset += 1;

        // Parse max_concurrency
        var max_concurrency: u32 = 10;
        if (offset + 4 <= value.len) {
            max_concurrency = std.mem.readInt(u32, value[offset..][0..4], .little);
            offset += 4;
        }

        // Parse process list
        var processes: std.ArrayList(ProcessInfo) = .{};
        if (offset + 2 <= value.len) {
            const process_count = std.mem.readInt(u16, value[offset..][0..2], .little);
            offset += 2;

            for (0..process_count) |_| {
                if (offset + 2 > value.len) break;
                const name_len = std.mem.readInt(u16, value[offset..][0..2], .little);
                offset += 2;
                if (offset + name_len + 1 > value.len) break;
                const name = value[offset .. offset + name_len];
                offset += name_len;
                const kind: ProcessKind = @enumFromInt(value[offset]);
                offset += 1;

                const owned_name = self.allocator.dupe(u8, name) catch continue;
                processes.append(self.allocator, .{
                    .name_owned = owned_name,
                    .kind = kind,
                }) catch {
                    self.allocator.free(owned_name);
                    continue;
                };
            }
        }

        // Parse optional metadata (JSON string)
        var metadata: ?[]const u8 = null;
        if (offset < value.len) {
            const has_metadata = value[offset];
            offset += 1;
            if (has_metadata == 1 and offset + 2 <= value.len) {
                const mlen = std.mem.readInt(u16, value[offset..][0..2], .little);
                offset += 2;
                if (offset + mlen <= value.len) {
                    metadata = value[offset .. offset + mlen];
                    offset += mlen;
                }
            }
        }

        // Parse optional machine_id
        var machine_id: ?[]const u8 = null;
        if (offset < value.len) {
            const has_machine_id = value[offset];
            offset += 1;
            if (has_machine_id == 1 and offset + 2 <= value.len) {
                const mid_len = std.mem.readInt(u16, value[offset..][0..2], .little);
                offset += 2;
                if (offset + mid_len <= value.len) {
                    machine_id = value[offset .. offset + mid_len];
                }
            }
        }

        // Remove old registration if exists
        if (self.workers.fetchRemove(req.key)) |old| {
            var old_val = old.value;
            self.freeWorkerRecord(&old_val);
        }

        const now_ms = std.time.milliTimestamp();
        const owned_id = self.allocator.dupe(u8, req.key) catch {
            for (processes.items) |p| self.allocator.free(p.name_owned);
            processes.deinit(self.allocator);
            return;
        };
        const owned_metadata: ?[]const u8 = if (metadata) |m|
            self.allocator.dupe(u8, m) catch null
        else
            null;
        const owned_machine_id: ?[]const u8 = if (machine_id) |mid|
            self.allocator.dupe(u8, mid) catch null
        else
            null;

        self.workers.put(owned_id, .{
            .id_owned = owned_id,
            .worker_type = worker_type,
            .status = .active,
            .metadata_owned = owned_metadata,
            .machine_id_owned = owned_machine_id,
            .processes = processes,
            .max_concurrency = max_concurrency,
            .registered_at_ms = now_ms,
            .last_heartbeat_ms = now_ms,
        }) catch {
            for (processes.items) |p| self.allocator.free(p.name_owned);
            processes.deinit(self.allocator);
            self.allocator.free(owned_id);
            if (owned_metadata) |m| self.allocator.free(m);
            if (owned_machine_id) |mid| self.allocator.free(mid);
        };
    }

    /// Heartbeat from a worker — lightweight keep-alive.
    /// value = [current_load:u32]
    /// Returns the worker's current status (so the SDK can detect draining).
    fn handleHeartbeat(self: *WorkerHandler, req: Request) ?WorkerStatus {
        const worker = self.workers.getPtr(req.key) orelse return null;
        worker.last_heartbeat_ms = std.time.milliTimestamp();

        // Don't reset draining→active — drain is sticky until deregister
        if (worker.status != .draining) {
            worker.status = .active;
        }

        if (req.value.len >= 4) {
            worker.current_load = std.mem.readInt(u32, req.value[0..4], .little);
        }

        return worker.status;
    }

    /// Drain a worker — marks it as draining so no new tasks are assigned.
    /// key = worker_id. Returns true if the worker was found.
    fn handleDrain(self: *WorkerHandler, req: Request) bool {
        const worker = self.workers.getPtr(req.key) orelse return false;
        worker.status = .draining;
        return true;
    }

    /// Deregister a worker. key = worker_id.
    fn handleDeregister(self: *WorkerHandler, req: Request) void {
        if (self.workers.fetchRemove(req.key)) |old| {
            var old_val = old.value;
            self.freeWorkerRecord(&old_val);
        }
    }

    /// Update status of stale workers (called periodically from shard tick).
    pub fn checkHealth(self: *WorkerHandler) void {
        const now_ms = std.time.milliTimestamp();
        var it = self.workers.iterator();
        while (it.next()) |entry| {
            const w = entry.value_ptr;
            if (w.status != .unhealthy and (now_ms - w.last_heartbeat_ms) > HEARTBEAT_TIMEOUT_MS) {
                w.status = .unhealthy;
            }
        }
    }

    /// Increment completed count for a worker + specific process (called by actions handler).
    pub fn recordCompletion(self: *WorkerHandler, worker_id: []const u8, process_name: ?[]const u8) void {
        if (self.workers.getPtr(worker_id)) |w| {
            w.tasks_completed += 1;
            if (w.current_load > 0) w.current_load -= 1;
            if (process_name) |pn| self.updateProcessRun(w, pn, false);
        }
    }

    /// Increment failed count for a worker + specific process (called by actions handler).
    pub fn recordFailure(self: *WorkerHandler, worker_id: []const u8, process_name: ?[]const u8) void {
        if (self.workers.getPtr(worker_id)) |w| {
            w.tasks_failed += 1;
            if (w.current_load > 0) w.current_load -= 1;
            if (process_name) |pn| self.updateProcessRun(w, pn, true);
        }
    }

    /// Increment current load for a worker (when task assigned).
    pub fn recordTaskAssigned(self: *WorkerHandler, worker_id: []const u8) void {
        if (self.workers.getPtr(worker_id)) |w| {
            w.current_load += 1;
        }
    }

    /// Check if a worker is draining (used by action_await to reject new tasks).
    pub fn isDraining(self: *WorkerHandler, worker_id: []const u8) bool {
        if (self.workers.get(worker_id)) |w| {
            return w.status == .draining;
        }
        return false;
    }

    /// Update per-process stats after a run.
    fn updateProcessRun(_: *WorkerHandler, w: *WorkerRecord, name: []const u8, is_failure: bool) void {
        const now_ms = std.time.milliTimestamp();
        for (w.processes.items) |*p| {
            if (std.mem.eql(u8, p.name_owned, name)) {
                if (is_failure) {
                    p.fail_count += 1;
                } else {
                    p.run_count += 1;
                }
                p.last_run_at_ms = now_ms;
                return;
            }
        }
    }

    // ── Serialization ───────────────────────────────────────────────────

    /// Compute wire size for a single worker record.
    fn workerWireSize(w: *const WorkerRecord) usize {
        var size: usize = 0;
        size += 2 + w.id_owned.len; // id_len + id
        size += 1 + 1; // type + status
        size += 8 + 8 + 4 + 4; // tasks_completed + tasks_failed + current_load + max_concurrency
        size += 8 + 8; // registered_at + last_heartbeat
        // processes
        size += 2; // process_count
        for (w.processes.items) |p| {
            size += 2 + p.name_owned.len; // name_len + name
            size += 1; // kind
            size += 8 + 8 + 8; // run_count + fail_count + last_run_at_ms
        }
        // metadata
        size += 1; // has_metadata
        if (w.metadata_owned) |m| {
            size += 2 + m.len;
        }
        // machine_id
        size += 1; // has_machine_id
        if (w.machine_id_owned) |mid| {
            size += 2 + mid.len;
        }
        return size;
    }

    /// Write a single worker record into buf at offset, returns new offset.
    fn writeWorkerRecord(buf: []u8, start: usize, w: *const WorkerRecord) usize {
        var offset = start;
        // id
        std.mem.writeInt(u16, buf[offset..][0..2], @intCast(w.id_owned.len), .little);
        offset += 2;
        @memcpy(buf[offset .. offset + w.id_owned.len], w.id_owned);
        offset += w.id_owned.len;
        // type + status
        buf[offset] = @intFromEnum(w.worker_type);
        offset += 1;
        buf[offset] = @intFromEnum(w.status);
        offset += 1;
        // counters
        std.mem.writeInt(u64, buf[offset..][0..8], w.tasks_completed, .little);
        offset += 8;
        std.mem.writeInt(u64, buf[offset..][0..8], w.tasks_failed, .little);
        offset += 8;
        std.mem.writeInt(u32, buf[offset..][0..4], w.current_load, .little);
        offset += 4;
        std.mem.writeInt(u32, buf[offset..][0..4], w.max_concurrency, .little);
        offset += 4;
        // timestamps
        std.mem.writeInt(i64, buf[offset..][0..8], w.registered_at_ms, .little);
        offset += 8;
        std.mem.writeInt(i64, buf[offset..][0..8], w.last_heartbeat_ms, .little);
        offset += 8;
        // processes
        std.mem.writeInt(u16, buf[offset..][0..2], @intCast(w.processes.items.len), .little);
        offset += 2;
        for (w.processes.items) |p| {
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(p.name_owned.len), .little);
            offset += 2;
            @memcpy(buf[offset .. offset + p.name_owned.len], p.name_owned);
            offset += p.name_owned.len;
            buf[offset] = @intFromEnum(p.kind);
            offset += 1;
            std.mem.writeInt(u64, buf[offset..][0..8], p.run_count, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], p.fail_count, .little);
            offset += 8;
            std.mem.writeInt(i64, buf[offset..][0..8], p.last_run_at_ms, .little);
            offset += 8;
        }
        // metadata
        if (w.metadata_owned) |m| {
            buf[offset] = 1;
            offset += 1;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(m.len), .little);
            offset += 2;
            @memcpy(buf[offset .. offset + m.len], m);
            offset += m.len;
        } else {
            buf[offset] = 0;
            offset += 1;
        }
        // machine_id
        if (w.machine_id_owned) |mid| {
            buf[offset] = 1;
            offset += 1;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(mid.len), .little);
            offset += 2;
            @memcpy(buf[offset .. offset + mid.len], mid);
            offset += mid.len;
        } else {
            buf[offset] = 0;
            offset += 1;
        }
        return offset;
    }

    /// Serialize worker list for wire response.
    /// Wire format: [count:u32](worker_record)* [has_more:u8][cursor_len:u16]
    /// worker_record: [id_len:u16][id][type:u8][status:u8]
    ///   [tasks_completed:u64][tasks_failed:u64][current_load:u32][max_concurrency:u32]
    ///   [registered_at:i64][last_heartbeat:i64]
    ///   [process_count:u16]([name_len:u16][name][kind:u8][run_count:u64][fail_count:u64][last_run_at:i64])*
    ///   [has_metadata:u8][metadata_len:u16][metadata]?
    fn serializeWorkerList(self: *WorkerHandler) ![]u8 {
        var total_size: usize = 4; // count
        var entry_count: u32 = 0;
        var it = self.workers.iterator();
        while (it.next()) |entry| {
            total_size += workerWireSize(entry.value_ptr);
            entry_count += 1;
        }
        total_size += 1 + 2; // has_more + cursor_len

        const buf = try self.allocator.alloc(u8, total_size);
        errdefer self.allocator.free(buf);
        var offset: usize = 0;

        std.mem.writeInt(u32, buf[offset..][0..4], entry_count, .little);
        offset += 4;

        var it2 = self.workers.iterator();
        while (it2.next()) |entry| {
            offset = writeWorkerRecord(buf, offset, entry.value_ptr);
        }

        buf[offset] = 0; // has_more = false
        offset += 1;
        std.mem.writeInt(u16, buf[offset..][0..2], 0, .little); // cursor_len = 0
        offset += 2;

        return buf;
    }

    /// Serialize a single worker's info.
    fn serializeWorkerInfo(self: *WorkerHandler, worker_id: []const u8) ![]u8 {
        const w = self.workers.getPtr(worker_id) orelse return error.NotFound;

        const total_size = workerWireSize(w);
        const buf = try self.allocator.alloc(u8, total_size);
        errdefer self.allocator.free(buf);
        _ = writeWorkerRecord(buf, 0, w);

        return buf;
    }

    /// ShardWalker LocalScanFn for worker_list.
    fn localScanWorkers(
        ctx: *anyopaque,
        _: []const u8,
        _: []const u8,
        _: ?[]const u8,
        _: u32,
    ) dispatcher_mod.NameWalker.ScanResult {
        const handler: *WorkerHandler = @ptrCast(@alignCast(ctx));
        const S = struct {
            threadlocal var name_buf: [1024][]const u8 = undefined;
        };

        var count: usize = 0;
        var it = handler.workers.iterator();
        while (it.next()) |entry| {
            if (count >= S.name_buf.len) break;
            S.name_buf[count] = entry.value_ptr.id_owned;
            count += 1;
        }

        return .{ .items = S.name_buf[0..count], .next_cursor = null };
    }
};
