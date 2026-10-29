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
//! A worker that misses its heartbeats is marked unhealthy and takes no
//! work; one silent past the removal grace is removed. Deregistration is
//! otherwise explicit.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const router = @import("../node/router.zig");
const ns_keys = @import("../namespace/handler.zig");
const ActionsHandler = @import("../actions/handler.zig").ActionsHandler;
const table_run = @import("../node/table_run.zig");
const gather_mod = @import("../node/gather.zig");
const schema = @import("../protocol/schema.zig");
const worker_ops = @import("../protocol/worker_ops.zig");

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
    /// The registry key (`WorkerHandler.workerKey`), owned.
    key_owned: []const u8,
    id_owned: []const u8,
    worker_type: WorkerType,
    status: WorkerStatus,
    /// Namespace this worker belongs to
    namespace_owned: []const u8,
    /// JSON metadata: labels, extra user data
    metadata_owned: ?[]const u8 = null,
    /// Machine/host identifier — groups workers on the same machine
    machine_id_owned: ?[]const u8 = null,
    /// Per-process tracking (actions handled, streams consumed)
    processes: std.ArrayList(ProcessInfo),
    tasks_completed: u64 = 0,
    tasks_failed: u64 = 0,
    /// Tasks in hand, as the worker reported in its last heartbeat, and 0
    /// once it is unhealthy. Worker-reported, not counted here: its runs
    /// complete and expire on other shards, which this one can't see.
    current_load: u32 = 0,
    max_concurrency: u32 = 10,
    registered_at_ms: i64,
    /// Wall clock, for display.
    last_heartbeat_ms: i64,
    /// Monotonic clock, for expiry: a wall-clock step must not mark every
    /// worker unhealthy, nor keep a dead one alive.
    last_seen_ms: u64,
};

comptime {
    // worker.list and worker.info answer with the record's own values.
    for (.{ .{ WorkerType, worker_ops.WorkerType }, .{ WorkerStatus, worker_ops.WorkerStatus }, .{ ProcessKind, worker_ops.ProcessKind } }) |pair| {
        const info = @typeInfo(pair[0]).@"enum";
        if (info.field_names.len != @typeInfo(pair[1]).@"enum".field_names.len) @compileError("worker.list's " ++ @typeName(pair[0]) ++ " differs from the record's");
        for (info.field_names, info.field_values) |name, value| {
            if (@backingInt(@field(pair[1], name)) != value) @compileError("worker.list's " ++ @typeName(pair[0]) ++ "." ++ name ++ " differs from the record's");
        }
    }
}

const ListResponse = table_run.op("worker.list").response;
const InfoResponse = table_run.op("worker.info").response;
const Workers = ListResponse.field_list[0].T;
const Row = worker_ops.WorkerRow;

pub const WorkerHandler = struct {
    allocator: Allocator,
    /// Keyed by `workerKey`: one worker id in two namespaces is two workers.
    workers: std.StringHashMap(WorkerRecord),
    /// worker.list builds its page here; allocated on first use.
    list_buf: ?[]u8 = null,
    /// worker.info's processes, encoded: one answer at a time.
    info_buf: ?[]u8 = null,

    /// A worker silent this long is unhealthy and takes no work: three
    /// missed beats at the SDKs' 30 s.
    heartbeat_timeout_ms: u64 = DEFAULT_HEARTBEAT_TIMEOUT_MS,
    /// A worker silent this long is removed, so ones that died without
    /// deregistering don't pile up.
    remove_after_ms: u64 = DEFAULT_REMOVE_AFTER_MS,

    const MAX_WORKERS: usize = 10_000;
    pub const DEFAULT_HEARTBEAT_TIMEOUT_MS: u64 = 90_000;
    pub const DEFAULT_REMOVE_AFTER_MS: u64 = 600_000;

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
        if (self.list_buf) |b| self.allocator.free(b);
        if (self.info_buf) |b| self.allocator.free(b);
    }

    fn freeWorkerRecord(self: *WorkerHandler, w: *WorkerRecord) void {
        for (w.processes.items) |p| {
            self.allocator.free(p.name_owned);
        }
        w.processes.deinit(self.allocator);
        self.allocator.free(w.key_owned);
        self.allocator.free(w.id_owned);
        self.allocator.free(w.namespace_owned);
        if (w.metadata_owned) |m| self.allocator.free(m);
        if (w.machine_id_owned) |mid| self.allocator.free(mid);
    }

    /// The registry key of worker `id` in `namespace`: "ns\x00id", or the
    /// id alone in "default". Null if too long to be one, or if the id
    /// holds a NUL and so could spell another namespace's worker.
    pub fn workerKey(buf: *[ns_keys.MAX_QUALIFIED_KEY]u8, namespace: []const u8, id: []const u8) ?[]const u8 {
        if (std.mem.indexOfScalar(u8, id, 0) != null) return null;
        return ns_keys.qualifyKey(buf, namespace, id) catch null;
    }

    /// Worker `id` as registered in `namespace`.
    pub fn find(self: *WorkerHandler, namespace: []const u8, id: []const u8) ?*WorkerRecord {
        var buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        return self.workers.getPtr(workerKey(&buf, namespace, id) orelse return null);
    }

    fn homeOf(namespace: []const u8) []const u8 {
        return if (namespace.len == 0) "default" else namespace;
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.worker_register, dispatchWorkerCmd, preRouteByWorker);
        dispatcher.registerWithRoute(.worker_heartbeat, dispatchWorkerCmd, preRouteByWorker);
        dispatcher.registerWithRoute(.worker_deregister, dispatchWorkerCmd, preRouteByWorker);
        dispatcher.registerWithRoute(.worker_drain, dispatchWorkerCmd, preRouteByWorker);
        dispatcher.register(.worker_list, table_run.handler("worker.list", WorkerHandler, runList));
        dispatcher.registerWithRoute(.worker_info, table_run.handler("worker.info", WorkerHandler, runInfo), preRouteByWorker);
    }

    pub fn preRouteByWorker(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    fn dispatchWorkerCmd(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));

        // A worker id is part of the registry key ("ns\x00id"), so one
        // holding a NUL could spell another namespace's worker.
        if (std.mem.indexOfScalar(u8, req.key, 0) != null) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "worker id must not contain NUL");
            return;
        }

        switch (op) {
            .worker_register => {
                if (shard.worker_handler.handleRegister(req)) |refused| {
                    shard.sendRefusal(conn, req.header.request_id, .bad_request, .{ .reason = refused.reason, .ran = .no, .message = refused.why });
                } else {
                    shard.sendOkResponse(conn, req.header.request_id, "");
                }
            },
            .worker_heartbeat => {
                if (shard.worker_handler.handleHeartbeat(req)) |status| {
                    var status_buf: [1]u8 = .{@backingInt(status)};
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
                    // Its parked awaits are answered now, not when they time
                    // out: they park here, where its registration is.
                    shard.waiter_pool.notifyAny(.action_await, ActionsHandler.resolveActionAwaitFn, @ptrCast(shard));
                    shard.sendOkResponse(conn, req.header.request_id, "");
                } else {
                    shard.sendErrorResponse(conn, req.header.request_id, .not_found, "worker not found");
                }
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
    ///   [has_metadata:u8]([metadata_len:u16][metadata])?
    ///   [has_machine_id:u8]([machine_id_len:u16][machine_id])?
    /// A value may stop after any whole field, and the rest take their
    /// defaults; one cut inside a field, a flag other than 0 or 1, or bytes
    /// past the end is refused, never read around.
    /// Why the worker was not registered, or null.
    fn handleRegister(self: *WorkerHandler, req: Request) ?Refused {
        if (req.key.len == 0) return .{ .why = "worker id is required" };
        var kbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const key = workerKey(&kbuf, req.namespace, req.key) orelse return .{ .why = "worker id too long, or holds a NUL" };
        if (self.workers.count() >= MAX_WORKERS and !self.workers.contains(key)) return .{ .why = "worker limit reached" };

        var reg = switch (parseRegistration(self.allocator, req.value) catch return .{ .why = "worker not registered: out of memory" }) {
            .refused => |refused| return refused,
            .ok => |r| r,
        };
        self.storeRegistration(key, req, &reg) catch {
            reg.deinit(self.allocator);
            return .{ .why = "worker not registered: out of memory" };
        };
        return null;
    }

    /// Why a registration was refused. A limit names its reason, so a
    /// client can tell which it hit without reading the message.
    const Refused = struct { reason: proto.Reason = .unclassified, why: []const u8 };

    const Registration = struct {
        worker_type: WorkerType = .action,
        max_concurrency: u32 = 10,
        processes: std.ArrayList(ProcessInfo) = .empty,
        /// Slices of the request.
        metadata: ?[]const u8 = null,
        machine_id: ?[]const u8 = null,

        fn deinit(self: *Registration, allocator: Allocator) void {
            for (self.processes.items) |p| allocator.free(p.name_owned);
            self.processes.deinit(allocator);
        }
    };

    const Parsed = union(enum) { ok: Registration, refused: Refused };

    fn parseRegistration(allocator: Allocator, value: []const u8) Allocator.Error!Parsed {
        var r: Reader = .{ .bytes = value };
        var reg: Registration = .{};
        errdefer reg.deinit(allocator);
        const refuse = struct {
            fn f(a: Allocator, g: *Registration, why: []const u8) Parsed {
                g.deinit(a);
                return .{ .refused = .{ .why = why } };
            }
        }.f;
        const pastLimit = struct {
            fn f(a: Allocator, g: *Registration, reason: proto.Reason, why: []const u8) Parsed {
                g.deinit(a);
                return .{ .refused = .{ .reason = reason, .why = why } };
            }
        }.f;

        if (r.done()) return .{ .ok = reg };
        reg.worker_type = std.enums.fromInt(WorkerType, r.byte().?) orelse return refuse(allocator, &reg, "unknown worker type");

        if (r.done()) return .{ .ok = reg };
        reg.max_concurrency = r.int(u32) orelse return refuse(allocator, &reg, "registration cut short in max_concurrency");

        if (r.done()) return .{ .ok = reg };
        const count = r.int(u16) orelse return refuse(allocator, &reg, "registration cut short in the process count");
        // worker.info answers with every process, so they must fit its bounds.
        if (count > worker_ops.MAX_PROCESSES) return pastLimit(allocator, &reg, .out_of_range, std.fmt.comptimePrint("more than {d} processes", .{worker_ops.MAX_PROCESSES}));
        var process_bytes: usize = 0;
        for (0..count) |_| {
            const name = r.prefixed() orelse return refuse(allocator, &reg, "registration cut short in its process list");
            const kind_byte = r.byte() orelse return refuse(allocator, &reg, "registration cut short in its process list");
            const kind = std.enums.fromInt(ProcessKind, kind_byte) orelse return refuse(allocator, &reg, "unknown process kind");
            process_bytes += worker_ops.ProcessRow.encodedSize(.{ .name = name, .kind = @fromBackingInt(kind_byte), .run_count = 0, .fail_count = 0, .last_run_at_ms = 0 }) catch
                return pastLimit(allocator, &reg, .field_too_long, std.fmt.comptimePrint("process name longer than {d} bytes", .{@import("../protocol/ops.zig").MAX_KEY}));
            if (process_bytes > worker_ops.Processes.byte_bound.?) return pastLimit(allocator, &reg, .field_too_long, "processes too long to list together");
            const owned = try allocator.dupe(u8, name);
            reg.processes.append(allocator, .{ .name_owned = owned, .kind = kind }) catch |err| {
                allocator.free(owned);
                return err;
            };
        }

        if (r.done()) return .{ .ok = reg };
        reg.metadata = switch (r.flag() orelse return refuse(allocator, &reg, "metadata flag must be 0 or 1")) {
            false => null,
            true => r.prefixed() orelse return refuse(allocator, &reg, "registration cut short in its metadata"),
        };

        if (r.done()) return .{ .ok = reg };
        reg.machine_id = switch (r.flag() orelse return refuse(allocator, &reg, "machine id flag must be 0 or 1")) {
            false => null,
            true => r.prefixed() orelse return refuse(allocator, &reg, "registration cut short in its machine id"),
        };
        // worker.list answers with it, so it must fit the list's bound.
        if (reg.machine_id) |m| if (m.len > worker_ops.MAX_MACHINE_ID)
            return pastLimit(allocator, &reg, .field_too_long, std.fmt.comptimePrint("machine id longer than {d} bytes", .{worker_ops.MAX_MACHINE_ID}));

        if (!r.done()) return refuse(allocator, &reg, "registration has bytes past its end");
        return .{ .ok = reg };
    }

    /// A cursor over a registration's bytes: each read takes a whole field or nothing.
    const Reader = struct {
        bytes: []const u8,
        pos: usize = 0,

        fn done(self: *const Reader) bool {
            return self.pos == self.bytes.len;
        }
        fn take(self: *Reader, n: usize) ?[]const u8 {
            if (self.bytes.len - self.pos < n) return null;
            defer self.pos += n;
            return self.bytes[self.pos..][0..n];
        }
        fn byte(self: *Reader) ?u8 {
            return (self.take(1) orelse return null)[0];
        }
        fn int(self: *Reader, comptime T: type) ?T {
            return std.mem.readInt(T, (self.take(@sizeOf(T)) orelse return null)[0..@sizeOf(T)], .little);
        }
        fn prefixed(self: *Reader) ?[]const u8 {
            return self.take(self.int(u16) orelse return null);
        }
        /// A 0 or 1 byte; null for anything else or nothing.
        fn flag(self: *Reader) ?bool {
            return switch (self.byte() orelse return null) {
                0 => false,
                1 => true,
                else => null,
            };
        }
    };

    /// Replace worker `key`'s registration with `reg`, which it takes on
    /// success. The old one goes only once the new one is whole.
    fn storeRegistration(self: *WorkerHandler, key: []const u8, req: Request, reg: *Registration) Allocator.Error!void {
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_id = try self.allocator.dupe(u8, req.key);
        errdefer self.allocator.free(owned_id);
        const owned_namespace = try self.allocator.dupe(u8, homeOf(req.namespace));
        errdefer self.allocator.free(owned_namespace);
        const owned_metadata: ?[]const u8 = if (reg.metadata) |m| try self.allocator.dupe(u8, m) else null;
        errdefer if (owned_metadata) |m| self.allocator.free(m);
        const owned_machine_id: ?[]const u8 = if (reg.machine_id) |m| try self.allocator.dupe(u8, m) else null;
        errdefer if (owned_machine_id) |m| self.allocator.free(m);
        // Room for the new record before the old one is dropped.
        try self.workers.ensureUnusedCapacity(1);

        if (self.workers.fetchRemove(key)) |old| {
            var old_val = old.value;
            self.freeWorkerRecord(&old_val);
        }
        const now_ms = @import("stdx").time.milliTimestamp();
        self.workers.putAssumeCapacityNoClobber(owned_key, .{
            .key_owned = owned_key,
            .id_owned = owned_id,
            .worker_type = reg.worker_type,
            .status = .active,
            .namespace_owned = owned_namespace,
            .metadata_owned = owned_metadata,
            .machine_id_owned = owned_machine_id,
            .processes = reg.processes,
            .max_concurrency = reg.max_concurrency,
            .registered_at_ms = now_ms,
            .last_heartbeat_ms = now_ms,
            .last_seen_ms = @import("stdx").time.monotonicMs(),
        });
    }

    /// Heartbeat from a worker — lightweight keep-alive.
    /// value = [current_load:u32]
    /// Returns the worker's current status (so the SDK can detect draining).
    fn handleHeartbeat(self: *WorkerHandler, req: Request) ?WorkerStatus {
        const worker = self.find(req.namespace, req.key) orelse return null;
        worker.last_heartbeat_ms = @import("stdx").time.milliTimestamp();
        worker.last_seen_ms = @import("stdx").time.monotonicMs();

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
        const worker = self.find(req.namespace, req.key) orelse return false;
        worker.status = .draining;
        return true;
    }

    /// Deregister a worker. key = worker_id.
    fn handleDeregister(self: *WorkerHandler, req: Request) void {
        var kbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const key = workerKey(&kbuf, req.namespace, req.key) orelse return;
        if (self.workers.fetchRemove(key)) |old| {
            var old_val = old.value;
            self.freeWorkerRecord(&old_val);
        }
    }

    /// Mark workers silent past the heartbeat timeout unhealthy, and remove
    /// those silent past the removal grace, by the monotonic clock `now_ms`.
    /// Returns how many changed.
    pub fn checkHealth(self: *WorkerHandler, now_ms: u64) usize {
        var changed: usize = 0;
        // Removed after the pass, which a removal would invalidate; any past
        // this many go on the next sweep.
        var gone: [256][]const u8 = undefined;
        var gone_count: usize = 0;
        var it = self.workers.iterator();
        while (it.next()) |entry| {
            const w = entry.value_ptr;
            const silent = now_ms -| w.last_seen_ms;
            if (silent > self.remove_after_ms) {
                if (gone_count == gone.len) continue;
                gone[gone_count] = entry.key_ptr.*;
                gone_count += 1;
            } else if (silent > self.heartbeat_timeout_ms and (w.status == .active or w.status == .idle)) {
                w.status = .unhealthy;
                // Its last report is stale, and it takes no work now.
                w.current_load = 0;
                changed += 1;
            }
        }
        for (gone[0..gone_count]) |key| {
            var record = self.workers.fetchRemove(key).?.value;
            self.freeWorkerRecord(&record);
        }
        return changed + gone_count;
    }

    /// Increment completed count for a worker + specific process (called by actions handler).
    pub fn recordCompletion(self: *WorkerHandler, namespace: []const u8, worker_id: []const u8, process_name: ?[]const u8) void {
        if (self.find(namespace, worker_id)) |w| {
            w.tasks_completed += 1;
            if (process_name) |pn| self.updateProcessRun(w, pn, false);
        }
    }

    /// Increment failed count for a worker + specific process (called by actions handler).
    pub fn recordFailure(self: *WorkerHandler, namespace: []const u8, worker_id: []const u8, process_name: ?[]const u8) void {
        if (self.find(namespace, worker_id)) |w| {
            w.tasks_failed += 1;
            if (process_name) |pn| self.updateProcessRun(w, pn, true);
        }
    }

    /// Why worker `worker_id` takes no new task, or null if it takes one.
    /// An unregistered worker takes tasks: registering is optional.
    pub fn refusesWork(self: *WorkerHandler, namespace: []const u8, worker_id: []const u8) ?proto.Refusal {
        const w = self.find(namespace, worker_id) orelse return null;
        return switch (w.status) {
            .draining => .{ .reason = .worker_draining, .ran = .no, .message = "worker is draining" },
            .unhealthy => .{ .reason = .worker_unhealthy, .ran = .no, .message = "worker missed its heartbeats; send one, or register again" },
            .active, .idle => null,
        };
    }

    /// Update per-process stats after a run.
    fn updateProcessRun(_: *WorkerHandler, w: *WorkerRecord, name: []const u8, is_failure: bool) void {
        const now_ms = @import("stdx").time.milliTimestamp();
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

    // ── INFO ────────────────────────────────────────────────────────────

    /// worker.info: the worker's row, metadata and processes, from the
    /// shard its id routes to.
    fn runInfo(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(InfoResponse) {
        return .{ .answer = try shard.worker_handler.info(req.namespace, req.key) };
    }

    fn info(self: *WorkerHandler, namespace: []const u8, worker_id: []const u8) Error!InfoResponse.Value {
        // A worker id is part of the registry key ("ns\x00id"), so one
        // holding a NUL could spell another namespace's worker.
        if (std.mem.indexOfScalar(u8, worker_id, 0) != null) return error.InvalidId;
        const w = self.find(namespace, worker_id) orelse return error.NotFound;
        const buf = self.info_buf orelse blk: {
            const b = try self.allocator.alloc(u8, worker_ops.Processes.byte_bound.?);
            self.info_buf = b;
            break :blk b;
        };
        var procs = worker_ops.Processes.Builder.init(buf);
        // Registration keeps a worker's processes within the answer's bounds.
        for (w.processes.items) |p| if (!(procs.add(.{
            .name = p.name_owned,
            .kind = @fromBackingInt(@backingInt(p.kind)),
            .run_count = p.run_count,
            .fail_count = p.fail_count,
            .last_run_at_ms = p.last_run_at_ms,
        }) catch unreachable)) unreachable;
        return .{ .worker = row(w), .metadata = w.metadata_owned, .processes = procs.value() };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    pub const Error = error{ OutOfMemory, GatherBusy, NotFound, InvalidId };

    pub fn refusal(err: Error) table_run.Refused {
        return switch (err) {
            error.OutOfMemory => .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "internal error: out of memory" } },
            error.GatherBusy => .{ .status = .overloaded, .refusal = .{ .reason = .shard_busy, .ran = .no, .message = "overloaded: too many reads across shards are under way; this request was not run — retry" } },
            error.NotFound => .{ .status = .not_found, .refusal = .{ .reason = .not_found, .ran = .no, .message = "worker not found" } },
            error.InvalidId => .{ .status = .bad_request, .refusal = .{ .reason = .malformed, .ran = .no, .message = "worker id must not contain NUL" } },
        };
    }

    /// worker.list: each shard holds the workers that registered on it, so
    /// every shard answers its own page after `cursor` and the pages merge.
    /// A worker.list another shard sent here is that gather's part.
    fn runList(shard: *Shard, conn: *Connection, req: Request, limit: u32, cursor: []const u8, action: []const u8) Error!table_run.Outcome(ListResponse) {
        const h = shard.worker_handler;
        if (conn == shard.forward_proxy) return .{ .answer = try h.page(req.namespace, limit, cursor, action) };

        const shards: usize = if (shard.peer_mailboxes) |m| m.len else 1;
        var parts: [gather_mod.MAX_PARTS]gather_mod.Part = undefined;
        const local = try encodeBody(h.allocator, try h.page(req.namespace, limit, cursor, action));
        const state = ListGather.create(h.allocator, limit) catch |err| {
            h.allocator.free(local);
            return err;
        };
        for (parts[0..shards], 0..) |*p, i| {
            p.* = .{ .owner = @intCast(i), .body = if (i == shard.id) local else req.value };
        }
        try shard.startGather(conn, req, parts[0..shards], &ListGather.op, state);
        return .wait;
    }

    /// This shard's workers in `namespace` after `cursor` that run `action`
    /// (any, if empty), in id order, as many as `limit` and the answer allow.
    /// The cursor is the last id given when more are left.
    fn page(self: *WorkerHandler, namespace: []const u8, limit: u32, cursor: []const u8, action: []const u8) Error!ListResponse.Value {
        var after: std.ArrayList(*const WorkerRecord) = .empty;
        defer after.deinit(self.allocator);
        var it = self.workers.valueIterator();
        while (it.next()) |w| {
            if (!std.mem.eql(u8, w.namespace_owned, homeOf(namespace))) continue;
            if (std.mem.order(u8, w.id_owned, cursor) != .gt) continue;
            if (action.len > 0 and !runsAction(w, action)) continue;
            try after.append(self.allocator, w);
        }
        std.mem.sort(*const WorkerRecord, after.items, {}, struct {
            fn lessThan(_: void, a: *const WorkerRecord, b: *const WorkerRecord) bool {
                return std.mem.order(u8, a.id_owned, b.id_owned) == .lt;
            }
        }.lessThan);

        var out = Workers.Builder.init(try self.listBuf());
        for (after.items) |w| {
            if (out.count == limit or !(out.add(row(w)) catch unreachable)) break; // a row never breaks its schema
        }
        const more = out.count < after.items.len;
        return .{ .workers = out.value(), .cursor = if (more) after.items[out.count - 1].id_owned else null };
    }

    fn runsAction(w: *const WorkerRecord, action: []const u8) bool {
        for (w.processes.items) |p| {
            if (p.kind == .action and std.mem.eql(u8, p.name_owned, action)) return true;
        }
        return false;
    }

    fn listBuf(self: *WorkerHandler) Error![]u8 {
        if (self.list_buf) |b| return b;
        const b = try self.allocator.alloc(u8, Workers.byte_bound.?);
        self.list_buf = b;
        return b;
    }

    fn row(w: *const WorkerRecord) Row.Value {
        return .{
            .id = w.id_owned,
            .worker_type = @fromBackingInt(@backingInt(w.worker_type)),
            .status = @fromBackingInt(@backingInt(w.status)),
            .current_load = w.current_load,
            .max_concurrency = w.max_concurrency,
            .tasks_completed = w.tasks_completed,
            .tasks_failed = w.tasks_failed,
            .registered_at_ms = w.registered_at_ms,
            .last_heartbeat_ms = w.last_heartbeat_ms,
            .machine_id = w.machine_id_owned,
        };
    }
};

/// A worker.list answer's body, owned: the part a shard gives its gather.
fn encodeBody(allocator: Allocator, v: ListResponse.Value) error{OutOfMemory}![]u8 {
    const size = ListResponse.encodedSize(v) catch unreachable; // a page keeps its bounds
    const buf = try allocator.alloc(u8, size);
    _ = ListResponse.encodeInto(buf, v) catch unreachable;
    return buf;
}

/// What a gathered worker.list keeps while its parts are out.
const ListGather = struct {
    limit: u32,

    const op: gather_mod.Op = .{ .merge = mergeAnswer, .deinit = deinit };

    fn create(allocator: Allocator, limit: u32) error{OutOfMemory}!*ListGather {
        const g = try allocator.create(ListGather);
        g.* = .{ .limit = limit };
        return g;
    }

    fn deinit(state: *anyopaque, allocator: Allocator) void {
        allocator.destroy(@as(*ListGather, @ptrCast(@alignCast(state))));
    }

    fn mergeAnswer(shard: *Shard, conn: *Connection, request_id: u64, state: *anyopaque, bodies: []const []const u8) anyerror!void {
        const g: *ListGather = @ptrCast(@alignCast(state));
        const h = shard.worker_handler;
        const merged = try mergePages(h.allocator, try h.listBuf(), g.limit, bodies);
        table_run.answer(ListResponse, shard, conn, request_id, merged);
    }
};

/// The shards' pages as one: each page is the first of its shard's rows
/// after one cursor, in id order, so rows are taken in id order up to the
/// smallest last id of a page that has more (past it, that page may have
/// left rows out), as many as `limit` and `buf` allow. A page that doesn't
/// decode is a server bug: `Corrupt`.
pub fn mergePages(allocator: Allocator, buf: []u8, limit: u32, bodies: []const []const u8) error{ Corrupt, OutOfMemory }!ListResponse.Value {
    var rows: std.ArrayList(Row.Value) = .empty;
    defer rows.deinit(allocator);
    var cut: ?[]const u8 = null;
    for (bodies) |b| {
        var diag: schema.Diagnostic = .{};
        const p = ListResponse.decodeAll(b, &diag) catch return error.Corrupt;
        if (p.cursor) |c| {
            if (cut == null or std.mem.order(u8, c, cut.?) == .lt) cut = c;
        }
        var it = p.workers.iterator();
        while (try it.next()) |r| try rows.append(allocator, r);
    }
    std.mem.sort(Row.Value, rows.items, {}, struct {
        fn lessThan(_: void, a: Row.Value, b: Row.Value) bool {
            return std.mem.order(u8, a.id, b.id) == .lt;
        }
    }.lessThan);

    var kept = rows.items;
    if (cut) |c| {
        for (rows.items, 0..) |r, i| {
            if (std.mem.order(u8, r.id, c) == .gt) {
                kept = rows.items[0..i];
                break;
            }
        }
    }
    var out = Workers.Builder.init(buf);
    for (kept) |r| {
        if (out.count == limit or !(out.add(r) catch return error.Corrupt)) break;
    }
    const more = out.count < rows.items.len or cut != null;
    if (more and out.count == 0) return error.Corrupt; // a page with more gave a row
    return .{ .workers = out.value(), .cursor = if (more) rows.items[out.count - 1].id else null };
}

/// `req` is refused with `reason` and `why`: a limit's reason is what lets
/// a client tell it apart without its message.
fn expectRefusedAs(h: *WorkerHandler, req: Request, reason: proto.Reason, why: []const u8) !void {
    const refused = h.handleRegister(req) orelse return error.NotRefused;
    try std.testing.expectEqual(reason, refused.reason);
    try std.testing.expectEqualStrings(why, refused.why);
}

fn testRequest(namespace: []const u8, id: []const u8) Request {
    const header = proto.RequestHeader.init(.worker_register, 0);
    return .{ .header = header, .namespace = namespace, .key = id, .value = "", .options = "" };
}

test "workers: one id in two namespaces is two workers, and an id holding a NUL names none" {
    var h = WorkerHandler.init(std.testing.allocator);
    defer h.deinit();
    try std.testing.expect(h.handleRegister(testRequest("b", "w")) == null);
    try std.testing.expect(h.find("b", "w") != null);
    try std.testing.expect(h.find("a", "w") == null);
    // In "default" keys are bare, so "b\x00w" would be b's worker w.
    try std.testing.expect(h.find("", "b\x00w") == null);
    try std.testing.expect(h.handleRegister(testRequest("", "b\x00w")) != null);
    try std.testing.expectEqual(@as(usize, 1), h.workers.count());
}

test "workers: an unknown worker type or process kind is refused, and nothing is kept" {
    var h = WorkerHandler.init(std.testing.allocator);
    defer h.deinit();

    var bad_type = testRequest("", "w1");
    bad_type.value = &.{2};
    try std.testing.expectEqualStrings("unknown worker type", h.handleRegister(bad_type).?.why);

    // [type][max_concurrency:u32][count:u16]([len:u16][name][kind])* — the second kind is unknown.
    var bad_kind = testRequest("", "w2");
    bad_kind.value = &.{ 0, 4, 0, 0, 0, 2, 0, 1, 0, 'a', 0, 1, 0, 'b', 0xee };
    try std.testing.expectEqualStrings("unknown process kind", h.handleRegister(bad_kind).?.why);
    // Not a limit: no reason of its own yet.
    try std.testing.expectEqual(proto.Reason.unclassified, h.handleRegister(bad_kind).?.reason);
    try std.testing.expectEqual(@as(usize, 0), h.workers.count());
}

test "workers: a machine id longer than worker.list's bound is refused" {
    var h = WorkerHandler.init(std.testing.allocator);
    defer h.deinit();
    // [type][max_concurrency:u32][count:u16][has_metadata][has_machine_id][len:u16][machine_id]
    var value: [11 + worker_ops.MAX_MACHINE_ID + 1]u8 = undefined;
    @memcpy(value[0..9], &[_]u8{ 0, 4, 0, 0, 0, 0, 0, 0, 1 });
    std.mem.writeInt(u16, value[9..11], worker_ops.MAX_MACHINE_ID + 1, .little);
    @memset(value[11..], 'm');
    var req = testRequest("", "w");
    req.value = &value;
    try expectRefusedAs(&h, req, .field_too_long, std.fmt.comptimePrint("machine id longer than {d} bytes", .{worker_ops.MAX_MACHINE_ID}));
    req.value = value[0 .. value.len - 1];
    std.mem.writeInt(u16, value[9..11], worker_ops.MAX_MACHINE_ID, .little);
    try std.testing.expect(h.handleRegister(req) == null);
    try std.testing.expectEqual(@as(usize, worker_ops.MAX_MACHINE_ID), h.find("", "w").?.machine_id_owned.?.len);
}

/// A registration of `count` action processes named `name_len` x's, then
/// `tail` (the metadata and machine id flags and fields).
fn processesValue(buf: []u8, count: u16, name_len: u16, tail: []const u8) []const u8 {
    var w: std.Io.Writer = .fixed(buf);
    w.writeAll(&.{ 0, 4, 0, 0, 0 }) catch unreachable;
    w.writeInt(u16, count, .little) catch unreachable;
    for (0..count) |_| {
        w.writeInt(u16, name_len, .little) catch unreachable;
        w.splatByteAll('x', name_len) catch unreachable;
        w.writeByte(0) catch unreachable;
    }
    w.writeAll(tail) catch unreachable;
    return w.buffered();
}

test "worker.info: the row, metadata and processes, as registered; an unknown worker and a NUL id are refused" {
    var h = WorkerHandler.init(std.testing.allocator);
    defer h.deinit();
    // Two processes, metadata "{}" and machine id "m1".
    var req = testRequest("b", "w");
    req.value = &.{ 0, 4, 0, 0, 0, 2, 0, 1, 0, 'a', 0, 1, 0, 's', 1, 1, 2, 0, '{', '}', 1, 2, 0, 'm', '1' };
    try std.testing.expect(h.handleRegister(req) == null);

    const got = try h.info("b", "w");
    try std.testing.expectEqualStrings("w", got.worker.id);
    try std.testing.expectEqual(@as(u32, 4), got.worker.max_concurrency);
    try std.testing.expectEqualStrings("m1", got.worker.machine_id.?);
    try std.testing.expectEqualStrings("{}", got.metadata.?);
    var it = got.processes.iterator();
    const a = (try it.next()).?;
    try std.testing.expectEqualStrings("a", a.name);
    try std.testing.expectEqual(worker_ops.ProcessKind.action, a.kind);
    const b = (try it.next()).?;
    try std.testing.expectEqualStrings("s", b.name);
    try std.testing.expectEqual(worker_ops.ProcessKind.stream_consumer, b.kind);
    try std.testing.expect((try it.next()) == null);

    // It encodes within its op's bounds, and decodes back to the same.
    var buf: [InfoResponse.max_size]u8 = undefined;
    const body = try InfoResponse.encodeInto(&buf, got);
    var diag: @import("../protocol/schema.zig").Diagnostic = .{};
    const back = try InfoResponse.decodeAll(body, &diag);
    try std.testing.expectEqualStrings("m1", back.worker.machine_id.?);
    try std.testing.expectEqual(@as(u32, 2), back.processes.count);

    try std.testing.expectError(error.NotFound, h.info("a", "w"));
    try std.testing.expectError(error.InvalidId, h.info("", "b\x00w"));
}

test "workers: a registration whose processes worker.info couldn't list is refused" {
    var h = WorkerHandler.init(std.testing.allocator);
    defer h.deinit();
    const buf = try std.testing.allocator.alloc(u8, 256 * 1024);
    defer std.testing.allocator.free(buf);
    var req = testRequest("", "w");

    req.value = processesValue(buf, worker_ops.MAX_PROCESSES + 1, 1, &.{});
    try expectRefusedAs(&h, req, .out_of_range, std.fmt.comptimePrint("more than {d} processes", .{worker_ops.MAX_PROCESSES}));

    req.value = processesValue(buf, 1, @intCast(@import("../protocol/ops.zig").MAX_KEY + 1), &.{});
    try expectRefusedAs(&h, req, .field_too_long, std.fmt.comptimePrint("process name longer than {d} bytes", .{@import("../protocol/ops.zig").MAX_KEY}));

    // Every name fits, but together they pass what an answer holds.
    req.value = processesValue(buf, worker_ops.MAX_PROCESSES, 200, &.{});
    try expectRefusedAs(&h, req, .field_too_long, "processes too long to list together");
    try std.testing.expectEqual(@as(usize, 0), h.workers.count());

    // At the bounds, it registers and lists.
    req.value = processesValue(buf, worker_ops.MAX_PROCESSES, 100, &.{});
    try std.testing.expect(h.handleRegister(req) == null);
    try std.testing.expectEqual(worker_ops.MAX_PROCESSES, (try h.info("", "w")).processes.count);
}

test "workers: processes landing exactly on worker.info's bound register and list whole; a byte more is refused" {
    const a = std.testing.allocator;
    const max_key = @import("../protocol/ops.zig").MAX_KEY;
    const per = worker_ops.ProcessRow.encodedSize(.{ .name = "", .kind = .action, .run_count = 0, .fail_count = 0, .last_run_at_ms = 0 }) catch unreachable;
    const full = 46;
    // 46 names of a key's length, then one sized to land on the bound.
    const last = worker_ops.Processes.byte_bound.? - full * (per + max_key) - per;
    try std.testing.expect(last <= max_key);

    var w: std.Io.Writer.Allocating = .init(a);
    defer w.deinit();
    for ([_]usize{ 0, 1 }) |over| {
        w.clearRetainingCapacity();
        const out = &w.writer;
        try out.writeAll(&.{ 0, 4, 0, 0, 0 });
        try out.writeInt(u16, full + 1, .little);
        for (0..full + 1) |i| {
            const len = if (i == full) last + over else max_key;
            try out.writeInt(u16, @intCast(len), .little);
            try out.splatByteAll('x', len);
            try out.writeByte(0);
        }
        // The largest metadata and machine id.
        try out.writeByte(1);
        try out.writeInt(u16, worker_ops.MAX_METADATA, .little);
        try out.splatByteAll('m', worker_ops.MAX_METADATA);
        try out.writeByte(1);
        try out.writeInt(u16, worker_ops.MAX_MACHINE_ID, .little);
        try out.splatByteAll('i', worker_ops.MAX_MACHINE_ID);

        var h = WorkerHandler.init(a);
        defer h.deinit();
        const id: [4000]u8 = @splat('w');
        var req = testRequest("", &id);
        req.value = w.written();
        if (over == 1) {
            try expectRefusedAs(&h, req, .field_too_long, "processes too long to list together");
            continue;
        }
        try std.testing.expect(h.handleRegister(req) == null);
        const got = try h.info("", &id);
        try std.testing.expectEqual(@as(u32, full + 1), got.processes.count);
        const size = try InfoResponse.encodedSize(got);
        try std.testing.expect(size <= InfoResponse.max_size);
        const buf = try a.alloc(u8, InfoResponse.max_size);
        defer a.free(buf);
        _ = try InfoResponse.encodeInto(buf, got);
    }
}

test "worker.info: a process's runs, failures and last run are what the worker's tasks reported" {
    var h = WorkerHandler.init(std.testing.allocator);
    defer h.deinit();
    var req = testRequest("", "w");
    req.value = &.{ 0, 4, 0, 0, 0, 2, 0, 1, 0, 'a', 0, 1, 0, 'b', 0 };
    try std.testing.expect(h.handleRegister(req) == null);
    const before = @import("stdx").time.milliTimestamp();
    h.recordCompletion("", "w", "a");
    h.recordCompletion("", "w", "a");
    h.recordFailure("", "w", "a");
    h.recordFailure("", "w", "b");

    const got = try h.info("", "w");
    try std.testing.expectEqual(@as(u64, 2), got.worker.tasks_completed);
    try std.testing.expectEqual(@as(u64, 2), got.worker.tasks_failed);
    var it = got.processes.iterator();
    const pa = (try it.next()).?;
    try std.testing.expectEqual(@as(u64, 2), pa.run_count);
    try std.testing.expectEqual(@as(u64, 1), pa.fail_count);
    try std.testing.expect(pa.last_run_at_ms >= before);
    const pb = (try it.next()).?;
    try std.testing.expectEqual(@as(u64, 0), pb.run_count);
    try std.testing.expectEqual(@as(u64, 1), pb.fail_count);
    try std.testing.expect(pb.last_run_at_ms >= before);
}

/// Shards' registries, each listed as its shard would answer a gather part.
const ListShards = struct {
    registries: [3]WorkerHandler,

    fn init(self: *ListShards) void {
        for (&self.registries) |*h| h.* = WorkerHandler.init(std.testing.allocator);
    }

    fn deinit(self: *ListShards) void {
        for (&self.registries) |*h| h.deinit();
    }

    fn add(self: *ListShards, shard: usize, namespace: []const u8, id: []const u8) !void {
        if (self.registries[shard].handleRegister(testRequest(namespace, id))) |refused| {
            std.debug.print("{s}\n", .{refused.why});
            return error.Refused;
        }
    }

    /// Register `id` running one process, `name`, of `kind`.
    fn addRunning(self: *ListShards, shard: usize, namespace: []const u8, id: []const u8, kind: ProcessKind, name: []const u8) !void {
        var buf: [64]u8 = undefined;
        var w: std.Io.Writer = .fixed(&buf);
        try w.writeAll(&.{ 0, 4, 0, 0, 0, 1, 0 });
        try w.writeInt(u16, @intCast(name.len), .little);
        try w.writeAll(name);
        try w.writeAll(&.{ @backingInt(kind), 0, 0 }); // no metadata, no machine id
        var req = testRequest(namespace, id);
        req.value = w.buffered();
        if (self.registries[shard].handleRegister(req)) |why| {
            std.debug.print("{s}\n", .{why.why});
            return error.Refused;
        }
    }

    /// Every page from the start, merged as a gather merges them; the ids
    /// in order, and the number of pages.
    fn listAll(self: *ListShards, namespace: []const u8, limit: u32, action: []const u8, ids: *std.ArrayList([]u8)) !usize {
        const a = std.testing.allocator;
        var buf: [Workers.byte_bound.?]u8 = undefined;
        var cursor: std.ArrayList(u8) = .empty;
        defer cursor.deinit(a);
        var pages: usize = 0;
        while (true) : (pages += 1) {
            // Every page moves the cursor on; one that doesn't would loop.
            if (pages > 10_000) return error.CursorStuck;
            var bodies: [3][]u8 = undefined;
            for (&self.registries, &bodies) |*h, *b| b.* = try encodeBody(a, try h.page(namespace, limit, cursor.items, action));
            defer for (bodies) |b| a.free(b);
            const merged = try mergePages(a, &buf, limit, &bodies);
            try std.testing.expect(merged.workers.count <= limit);
            var it = merged.workers.iterator();
            while (try it.next()) |r| try ids.append(a, try a.dupe(u8, r.id));
            const next = merged.cursor orelse return pages + 1;
            try std.testing.expect(std.mem.order(u8, next, cursor.items) == .gt);
            cursor.clearRetainingCapacity();
            try cursor.appendSlice(a, next);
        }
    }
};

fn freeIds(ids: *std.ArrayList([]u8)) void {
    for (ids.items) |id| std.testing.allocator.free(id);
    ids.deinit(std.testing.allocator);
}

test "worker.list: pages over shards whose ids interleave give every worker once, in id order, whatever the limit" {
    var s: ListShards = undefined;
    s.init();
    defer s.deinit();
    // Ids spread so each page boundary falls between two shards' ids.
    var name: [8]u8 = undefined;
    for (0..20) |i| try s.add(i * 7 % 3, "ns", try std.fmt.bufPrint(&name, "w{d:0>2}", .{i}));
    try s.add(0, "other", "w99");

    for ([_]u32{ 1, 2, 3, 7, 20, worker_ops.MAX_LIST }) |limit| {
        var ids: std.ArrayList([]u8) = .empty;
        defer freeIds(&ids);
        const pages = try s.listAll("ns", limit, "", &ids);
        try std.testing.expectEqual(@as(usize, 20), ids.items.len);
        for (ids.items, 0..) |id, i| try std.testing.expectEqualStrings(try std.fmt.bufPrint(&name, "w{d:0>2}", .{i}), id);
        try std.testing.expectEqual((20 + limit - 1) / limit, pages);
    }
}

test "worker.list: workers all on one shard are paged to the last, though the other shards have none" {
    var s: ListShards = undefined;
    s.init();
    defer s.deinit();
    var name: [8]u8 = undefined;
    for (0..20) |i| try s.add(1, "ns", try std.fmt.bufPrint(&name, "w{d:0>2}", .{i}));
    for ([_]u32{ 1, 5, 20 }) |limit| {
        var ids: std.ArrayList([]u8) = .empty;
        defer freeIds(&ids);
        _ = try s.listAll("ns", limit, "", &ids);
        try std.testing.expectEqual(@as(usize, 20), ids.items.len);
    }
}

test "worker.list: an action filter pages only that action's workers, each page full, to the last" {
    var s: ListShards = undefined;
    s.init();
    defer s.deinit();
    var name: [8]u8 = undefined;
    // Every third runs "a"; the rest run "b", or a stream consumer named "a",
    // or are in another namespace, interleaved across the shards.
    for (0..30) |i| {
        const id = try std.fmt.bufPrint(&name, "w{d:0>2}", .{i});
        const shard = i * 7 % 3;
        switch (i % 3) {
            0 => try s.addRunning(shard, "ns", id, .action, "a"),
            1 => try s.addRunning(shard, "ns", id, .action, "b"),
            else => try s.addRunning(shard, "ns", id, .stream_consumer, "a"),
        }
    }
    try s.addRunning(0, "other", "w99", .action, "a");

    for ([_]u32{ 1, 3, 4, 10 }) |limit| {
        var ids: std.ArrayList([]u8) = .empty;
        defer freeIds(&ids);
        const pages = try s.listAll("ns", limit, "a", &ids);
        try std.testing.expectEqual(@as(usize, 10), ids.items.len);
        for (ids.items, 0..) |id, i| try std.testing.expectEqualStrings(try std.fmt.bufPrint(&name, "w{d:0>2}", .{i * 3}), id);
        try std.testing.expectEqual((10 + limit - 1) / limit, pages);
    }

    var ids: std.ArrayList([]u8) = .empty;
    defer freeIds(&ids);
    _ = try s.listAll("ns", 10, "none", &ids);
    try std.testing.expectEqual(@as(usize, 0), ids.items.len);
}

test "worker.list: a page cut short by the answer's room still loses no worker" {
    var s: ListShards = undefined;
    s.init();
    defer s.deinit();
    // Ids this long fill a page well before its limit, so shards' pages
    // stop at different ids.
    var name: [ops_max_key]u8 = undefined;
    @memset(&name, 'x');
    for (0..150) |i| {
        _ = std.fmt.bufPrint(name[name.len - 4 ..], "{d:0>4}", .{i}) catch unreachable;
        try s.add(i * 5 % 3, "ns", &name);
    }
    var ids: std.ArrayList([]u8) = .empty;
    defer freeIds(&ids);
    const pages = try s.listAll("ns", worker_ops.MAX_LIST, "", &ids);
    try std.testing.expect(pages > 1);
    try std.testing.expectEqual(@as(usize, 150), ids.items.len);
    for (ids.items, 0..) |id, i| {
        _ = std.fmt.bufPrint(name[name.len - 4 ..], "{d:0>4}", .{i}) catch unreachable;
        try std.testing.expectEqualStrings(&name, id);
    }
}

/// A page of workers with `ids`, and `cursor`, as a shard would answer it.
fn testPage(buf: []u8, ids: []const []const u8, cursor: ?[]const u8) ![]const u8 {
    var rows = Workers.Builder.init(buf[buf.len / 2 ..]);
    for (ids) |id| try std.testing.expect(try rows.add(.{ .id = id, .worker_type = .action, .status = .active, .current_load = 0, .max_concurrency = 1, .tasks_completed = 0, .tasks_failed = 0, .registered_at_ms = 0, .last_heartbeat_ms = 0, .machine_id = null }));
    return ListResponse.encodeInto(buf[0 .. buf.len / 2], .{ .workers = rows.value(), .cursor = cursor });
}

test "worker.list: a merge stops at the last id of a page that stopped early, where that page may have left workers out" {
    var a_buf: [1024]u8 = undefined;
    var b_buf: [1024]u8 = undefined;
    var buf: [Workers.byte_bound.?]u8 = undefined;
    // Shard a stopped after a3 (its page was full); shard b gave all it has.
    const pages = [_][]const u8{
        try testPage(&a_buf, &.{ "a1", "a3" }, "a3"),
        try testPage(&b_buf, &.{ "a2", "a4", "a5" }, null),
    };
    const merged = try mergePages(std.testing.allocator, &buf, 10, &pages);
    var it = merged.workers.iterator();
    for ([_][]const u8{ "a1", "a2", "a3" }) |want| try std.testing.expectEqualStrings(want, (try it.next()).?.id);
    try std.testing.expectEqual(@as(?Row.Value, null), try it.next());
    try std.testing.expectEqualStrings("a3", merged.cursor.?);
}

test "worker.list: a namespace with no workers is one empty page" {
    var s: ListShards = undefined;
    s.init();
    defer s.deinit();
    try s.add(1, "other", "w1");
    var ids: std.ArrayList([]u8) = .empty;
    defer freeIds(&ids);
    try std.testing.expectEqual(@as(usize, 1), try s.listAll("ns", 10, "", &ids));
    try std.testing.expectEqual(@as(usize, 0), ids.items.len);
}

test "worker.list: a page that doesn't decode is refused before anything is answered" {
    var buf: [Workers.byte_bound.?]u8 = undefined;
    try std.testing.expectError(error.Corrupt, mergePages(std.testing.allocator, &buf, 10, &.{"\x01"}));
}

// A worker id is a qualified registry key at most; the id alone is shorter
// by its namespace and separator.
const ops_max_key = @import("../protocol/ops.zig").MAX_KEY - 8;

test "workers: a silent worker is marked unhealthy and refused work, then removed; a heartbeat brings it back" {
    var h = WorkerHandler.init(std.testing.allocator);
    defer h.deinit();
    h.heartbeat_timeout_ms = 100;
    h.remove_after_ms = 1000;
    for ([_][]const u8{ "a", "b", "c" }) |id| try std.testing.expect(h.handleRegister(testRequest("", id)) == null);
    const t0: u64 = 1_000_000;
    for ([_][]const u8{ "a", "b", "c" }) |id| h.find("", id).?.last_seen_ms = t0;
    h.find("", "a").?.current_load = 4;
    h.find("", "c").?.status = .draining;

    try std.testing.expectEqual(@as(usize, 0), h.checkHealth(t0 + 100));
    try std.testing.expect(h.refusesWork("", "a") == null);

    // Silent past the timeout: a and b unhealthy; c stays draining.
    try std.testing.expectEqual(@as(usize, 2), h.checkHealth(t0 + 101));
    try std.testing.expectEqual(WorkerStatus.unhealthy, h.find("", "a").?.status);
    try std.testing.expectEqual(@as(u32, 0), h.find("", "a").?.current_load);
    try std.testing.expectEqual(WorkerStatus.draining, h.find("", "c").?.status);
    try std.testing.expectEqual(proto.Reason.worker_unhealthy, h.refusesWork("", "a").?.reason);
    try std.testing.expectEqual(proto.Reason.worker_draining, h.refusesWork("", "c").?.reason);
    // An unregistered worker takes work.
    try std.testing.expect(h.refusesWork("", "nobody") == null);

    // A heartbeat makes a healthy again, from then on.
    try std.testing.expectEqual(WorkerStatus.active, h.handleHeartbeat(testRequest("", "a")).?);
    h.find("", "a").?.last_seen_ms = t0 + 950;
    try std.testing.expect(h.refusesWork("", "a") == null);

    // Silent past the grace, b and c are removed, draining or not; a isn't.
    try std.testing.expectEqual(@as(usize, 2), h.checkHealth(t0 + 1001));
    try std.testing.expectEqual(@as(usize, 1), h.workers.count());
    try std.testing.expect(h.find("", "a") != null);
}

test "workers: current_load is what the worker last reported" {
    var h = WorkerHandler.init(std.testing.allocator);
    defer h.deinit();
    try std.testing.expect(h.handleRegister(testRequest("", "w")) == null);
    var beat = testRequest("", "w");
    beat.value = &.{ 3, 0, 0, 0 };
    _ = h.handleHeartbeat(beat);
    h.recordCompletion("", "w", null);
    h.recordFailure("", "w", null);
    try std.testing.expectEqual(@as(u32, 3), h.find("", "w").?.current_load);
    try std.testing.expectEqual(@as(u64, 1), h.find("", "w").?.tasks_completed);
}

test "workers: expiry reads the monotonic clock, so a wall-clock step expires no one" {
    var h = WorkerHandler.init(std.testing.allocator);
    defer h.deinit();
    try std.testing.expect(h.handleRegister(testRequest("", "w")) == null);
    const w = h.find("", "w").?;
    // The wall clock stepped: the last heartbeat now reads a day ago.
    w.last_heartbeat_ms -= std.time.ms_per_day;
    try std.testing.expectEqual(@as(usize, 0), h.checkHealth(w.last_seen_ms + 1000));
    try std.testing.expectEqual(WorkerStatus.active, w.status);
}

test "workers: a registration cut inside a field, with a bad flag, or with bytes past its end is refused, not read around" {
    var h = WorkerHandler.init(std.testing.allocator);
    defer h.deinit();
    // [type][max_concurrency:u32][count:u16]([len:u16][name][kind])*[has_metadata]([len:u16][metadata])?[has_machine_id]([len:u16][machine_id])?
    const Case = struct { value: []const u8, why: []const u8 };
    for ([_]Case{
        .{ .value = &.{ 0, 4, 0 }, .why = "registration cut short in max_concurrency" },
        .{ .value = &.{ 0, 4, 0, 0, 0, 1 }, .why = "registration cut short in the process count" },
        .{ .value = &.{ 0, 4, 0, 0, 0, 2, 0, 1, 0, 'a', 0 }, .why = "registration cut short in its process list" },
        .{ .value = &.{ 0, 4, 0, 0, 0, 1, 0, 3, 0, 'a' }, .why = "registration cut short in its process list" },
        .{ .value = &.{ 0, 4, 0, 0, 0, 0, 0, 2 }, .why = "metadata flag must be 0 or 1" },
        .{ .value = &.{ 0, 4, 0, 0, 0, 0, 0, 1, 9, 0, '{' }, .why = "registration cut short in its metadata" },
        .{ .value = &.{ 0, 4, 0, 0, 0, 0, 0, 0, 7 }, .why = "machine id flag must be 0 or 1" },
        .{ .value = &.{ 0, 4, 0, 0, 0, 0, 0, 0, 1, 5, 0, 'h' }, .why = "registration cut short in its machine id" },
        .{ .value = &.{ 0, 4, 0, 0, 0, 0, 0, 0, 0, 0xff }, .why = "registration has bytes past its end" },
    }) |c| {
        var req = testRequest("", "w");
        req.value = c.value;
        try std.testing.expectEqualStrings(c.why, h.handleRegister(req).?.why);
    }
    try std.testing.expectEqual(@as(usize, 0), h.workers.count());

    // Whole, it is kept in full; one that stops after a whole field takes defaults.
    var full = testRequest("", "w");
    full.value = &.{ 1, 4, 0, 0, 0, 1, 0, 1, 0, 'a', 1, 1, 2, 0, '{', '}', 1, 1, 0, 'h' };
    try std.testing.expect(h.handleRegister(full) == null);
    const w = h.find("", "w").?;
    try std.testing.expectEqual(WorkerType.stream, w.worker_type);
    try std.testing.expectEqual(@as(u32, 4), w.max_concurrency);
    try std.testing.expectEqualStrings("a", w.processes.items[0].name_owned);
    try std.testing.expectEqualStrings("{}", w.metadata_owned.?);
    try std.testing.expectEqualStrings("h", w.machine_id_owned.?);
    var short = testRequest("", "v");
    short.value = &.{ 0, 7, 0, 0, 0 };
    try std.testing.expect(h.handleRegister(short) == null);
    try std.testing.expectEqual(@as(u32, 7), h.find("", "v").?.max_concurrency);
}

test "workers: a re-registration short of memory at any point keeps the old one whole" {
    const run = struct {
        fn f(allocator: Allocator) !void {
            var h = WorkerHandler.init(allocator);
            defer h.deinit();
            var first = testRequest("", "w");
            first.value = &.{ 0, 1, 0, 0, 0, 1, 0, 1, 0, 'a', 0, 0, 0 };
            if (h.handleRegister(first)) |_| return error.OutOfMemory;
            var again = testRequest("", "w");
            again.value = &.{ 0, 9, 0, 0, 0, 1, 0, 1, 0, 'b', 0, 1, 2, 0, '{', '}', 1, 1, 0, 'h' };
            if (h.handleRegister(again)) |refused| {
                try std.testing.expectEqualStrings("worker not registered: out of memory", refused.why);
                // The old registration is still there, untouched.
                const w = h.find("", "w").?;
                try std.testing.expectEqual(@as(u32, 1), w.max_concurrency);
                try std.testing.expectEqualStrings("a", w.processes.items[0].name_owned);
                return error.OutOfMemory;
            }
            const w = h.find("", "w").?;
            try std.testing.expectEqual(@as(u32, 9), w.max_concurrency);
            try std.testing.expectEqual(@as(usize, 1), w.processes.items.len);
            try std.testing.expectEqualStrings("b", w.processes.items[0].name_owned);
            try std.testing.expectEqualStrings("{}", w.metadata_owned.?);
            try std.testing.expectEqualStrings("h", w.machine_id_owned.?);
        }
    }.f;
    try std.testing.checkAllAllocationFailures(std.testing.allocator, run, .{});
}

test "workers: a process name that can't be copied fails the registration, not just that process" {
    // Only the name's copy fails: a later allocation can't stand in for it.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const value = [_]u8{ 0, 1, 0, 0, 0, 1, 0, 1, 0, 'a', 0 };
    try std.testing.expectError(error.OutOfMemory, WorkerHandler.parseRegistration(failing.allocator(), &value));
    try std.testing.expect(failing.has_induced_failure);
}
