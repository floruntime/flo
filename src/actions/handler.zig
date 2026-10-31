//! The action family on a shard: its ops, and what a leader does that the
//! log can't: choosing which ready run a worker gets, holding a worker's
//! await until there is one, ending leases past their deadline, and telling
//! a workflow on another shard that a run it waits on finished.
//!
//! Every action and run lives in the contract store (`actions/machine.zig`),
//! changed only by applied entries, so followers, a restarted node and a new
//! leader hold the same runs at the same deliveries. What this keeps is the
//! leader's bookkeeping for its own proposals, dropped when it stops leading.

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const schema = @import("../protocol/schema.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const router = @import("../node/router.zig");
const run_id_mod = @import("../node/run_id.zig");
const table_run = @import("../node/table_run.zig");
const gather_mod = @import("../node/gather.zig");
const waiter_pool_mod = @import("../node/waiter_pool.zig");
const ReplyTo = @import("../node/reply_to.zig").ReplyTo;
const WorkerHandler = @import("../worker/handler.zig").WorkerHandler;
const action_ops = @import("../protocol/action_ops.zig");
const ae = @import("../protocol/action_entries.zig");
const machine = @import("machine.zig");
const view_mod = @import("../contract/view.zig");
const store_mod = @import("../contract/store.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");

const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const Waiter = waiter_pool_mod.Waiter;
const Result = persistence.Result;
const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const Flags = entry_mod.Flags;
const CommandResult = result_mod.CommandResult;
const ActionRunStatus = CommandResult.ActionRunStatus;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const ReadView = view_mod.ReadView;
const Run = machine.Run.Value;

/// An action's lease when its register names none.
pub const DEFAULT_LEASE_MS: u64 = 5 * 60 * 1000;

/// The most actions one shard holds.
const MAX_ACTIONS: usize = 10_000;

/// The most ready runs one selection looks at before answering none: runs
/// the worker's labels don't match are skipped, never scanned without end.
const MAX_LOOKED_AT: usize = 256;

/// How often the expire duty runs. A lease holds at most three deadline
/// keys only while the duty drops past-due keys within half the shortest
/// lease (`actions/machine.zig`).
pub const DUTY_INTERVAL_MS: u64 = 1000;
comptime {
    std.debug.assert(DUTY_INTERVAL_MS * 2 <= ae.MIN_LEASE_MS);
}

/// A new leader waits this long, or a lease if shorter, past a lease's
/// deadline before ending it: a holder that touched the old leader in time
/// may not have reached this one yet.
const MAX_GRACE_MS: u64 = 30_000;

const AwaitResponse = table_run.op("action.await").response;
const DeliverResponse = table_run.op("action.deliver").response;
const AwaitRequest = table_run.op("action.await").request;
const DeliverRequest = table_run.op("action.deliver").request;
const TouchResponse = table_run.op("action.touch").response;
const EmptyResponse = table_run.op("action.complete").response;
const StatusResponse = table_run.op("action.status").response;
const Task = action_ops.Task.Value;

comptime {
    // An await's answer is the answer its owner's deliver gave, as is.
    std.debug.assert(AwaitResponse.max_size == DeliverResponse.max_size);
    std.debug.assert(std.mem.eql(u8, AwaitResponse.field_list[0].name, DeliverResponse.field_list[0].name) and AwaitResponse.field_list[0].T == DeliverResponse.field_list[0].T);
}

/// The store: the shard's default partition holds every family.
fn storeOf(shard: *Shard) *store_mod.Store {
    return &shard.defaultPartition().store;
}

fn readOf(shard: *Shard) ReadView {
    return .{ .store = storeOf(shard) };
}

/// A bare namespace is "default", as the entries carry it.
fn homeOf(namespace: []const u8) []const u8 {
    return if (namespace.len == 0) "default" else namespace;
}

fn wallNs() u64 {
    return @intCast(@max(0, @import("stdx").time.nanoTimestamp()));
}

fn nsToMs(ns: u64) i64 {
    return @intCast(ns / std.time.ns_per_ms);
}

/// `k` in `ns`, as a family stores it, in `buf`.
fn storeKey(buf: []u8, ns: []const u8, k: []const u8) ?[]const u8 {
    if (ns.len > view_mod.MAX_NAMESPACE) return null;
    return view_mod.key(buf, ns, k) catch null;
}

const KEY_BUF = 1 + view_mod.MAX_NAMESPACE + machine.MAX_KEY_LEN;

fn getRun(shard: *Shard, ns: []const u8, run_id: []const u8) ?Run {
    var b: [KEY_BUF]u8 = undefined;
    const k = storeKey(&b, ns, run_id) orelse return null;
    const r = readOf(shard).get(machine.Runs, k) catch return null;
    return if (r) |x| x.v else null;
}

fn getDef(shard: *Shard, ns: []const u8, name: []const u8) ?machine.Def.Value {
    var b: [KEY_BUF]u8 = undefined;
    const k = storeKey(&b, ns, name) orelse return null;
    const d = readOf(shard).get(machine.Defs, k) catch return null;
    return if (d) |x| x.v else null;
}

/// The shard that owns action `name` in `ns`: invokes are routed there, so
/// its runs are there.
fn ownerOf(shard: *Shard, ns: []const u8, name: []const u8) u16 {
    return switch (shard.router.route(router.hashKeyWithNamespace(ns, name))) {
        .local => @intCast(shard.id),
        .shard => |t| t.shard_id,
    };
}

/// Propose a schema'd action entry.
fn propose(shard: *Shard, comptime etype: EntryType, body: anytype) !persistence.ProposeResult {
    const P = ae.Payload(@backingInt(etype));
    const size = try P.encodedSize(body);
    const buf = try shard.allocator.alloc(u8, size);
    defer shard.allocator.free(buf);
    return shard.raft_node.propose(etype, Flags.NONE, try P.encodeInto(buf, body));
}

pub const ActionsHandler = struct {
    allocator: Allocator,

    /// Runs a deliver is proposed for and hasn't applied, by store key: no
    /// other await is given them meanwhile.
    reserved: std.StringHashMapUnmanaged(void) = .empty,
    /// Awaits and asks answered when their deliver applies.
    pending: std.ArrayListUnmanaged(Pending) = .empty,
    /// Runs with an extend in flight, by store key: touches between answer
    /// with the lease as it stands.
    extending: std.StringHashMapUnmanaged(void) = .empty,
    /// `holder\x00request_key` → the delivery it got, so a retried await
    /// gets that one again while its lease runs.
    remembered: std.StringHashMapUnmanaged(Remembered) = .empty,
    /// The expire proposed and not yet applied: one at a time.
    expiring: ?u64 = null,
    /// The term the duty last ran in, and when it first ran in it.
    duty_term: u64 = 0,
    duty_since_ns: u64 = 0,
    /// Awaits woken this pass, to run again once the pass is over.
    reruns: std.ArrayListUnmanaged(Rerun) = .empty,
    /// How runs a workflow here waits on ended, sent by their own shards.
    finished: std.StringHashMapUnmanaged(Finished) = .empty,
    /// Messages for other shards the inbox had no room for; sent again each
    /// duty tick.
    unsent: std.ArrayListUnmanaged(Unsent) = .empty,
    /// action.status copies a run's output and error here: the answer is
    /// encoded after the store may have moved.
    status_buf: ?[]u8 = null,

    const Pending = struct {
        index: u64,
        term: u64,
        reply_to: ReplyTo,
        request_id: u64,
        /// For a retried await to find this delivery again.
        remember: ?[]u8,
    };

    const Remembered = struct { ns: []u8, run: []u8, token: ae.Token.Value, expiry_ns: u64 };

    const Rerun = struct { reply_to: ReplyTo, request_id: u64, bytes: []u8 };

    pub const Finished = struct { status: ActionRunStatus, outcome: []u8, output: []u8 };

    const Unsent = struct { target: u16, tag: @import("../node/inbox.zig").Tag, bytes: []u8 };

    pub fn init(allocator: Allocator) ActionsHandler {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ActionsHandler) void {
        self.forgetLeading();
        self.reserved.deinit(self.allocator);
        self.pending.deinit(self.allocator);
        self.extending.deinit(self.allocator);
        self.remembered.deinit(self.allocator);
        for (self.reruns.items) |r| self.allocator.free(r.bytes);
        self.reruns.deinit(self.allocator);
        var it = self.finished.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.outcome);
            self.allocator.free(e.value_ptr.output);
        }
        self.finished.deinit(self.allocator);
        for (self.unsent.items) |u| self.allocator.free(u.bytes);
        self.unsent.deinit(self.allocator);
        if (self.status_buf) |b| self.allocator.free(b);
    }

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.action_register, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.action_invoke, dispatchAction, preRouteByAction);
        // The run id's bits name the partition the run lives on.
        dispatcher.registerWithRoute(.action_status, table_run.handler("action.status", ActionsHandler, runStatus), run_id_mod.preRouteByRunId);
        dispatcher.registerWalk(.action_list, dispatchAction, localScanActions);
        dispatcher.registerWithRoute(.action_delete, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.action_list_runs, dispatchAction, preRouteByAction);
        // An await runs where its worker is registered: its labels, whether
        // it drains, the actions it runs. The runs it takes live on their
        // actions' shards, which it asks.
        dispatcher.registerWithRoute(.action_await, table_run.handler("action.await", ActionsHandler, runAwait), table_run.routeByKey("action.await"));
        dispatcher.registerWithRoute(.action_deliver, table_run.handler("action.deliver", ActionsHandler, runDeliver), table_run.routeByKey("action.deliver"));
        dispatcher.registerWithRoute(.action_complete, table_run.handler("action.complete", ActionsHandler, runComplete), run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.action_fail, table_run.handler("action.fail", ActionsHandler, runFail), run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.action_touch, table_run.handler("action.touch", ActionsHandler, runTouch), run_id_mod.preRouteByRunId);
    }

    fn preRouteByAction(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    // ── Errors ──────────────────────────────────────────────────────────

    pub const Error = error{ NotFound, OutOfMemory, StoredTooLong, NotLeading, Stale, Unproposed, NameHasNul };

    pub fn refusal(err: Error) table_run.Refused {
        return switch (err) {
            error.NotFound => .{ .status = .not_found, .refusal = .{ .reason = .not_found, .ran = .no, .message = machine.NO_RUN } },
            error.OutOfMemory => .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "internal error: out of memory" } },
            error.StoredTooLong => .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "internal error: run output or error longer than status can carry" } },
            error.NotLeading => .{ .status = .unavailable, .refusal = .{ .reason = .leader_unknown, .ran = .no, .message = "unavailable: this node's leadership isn't settled yet — retry" } },
            error.Stale => .{ .status = .conflict, .refusal = .{ .reason = .stale_report, .ran = .no, .message = "stale report: the run was delivered again since" } },
            error.Unproposed => .{ .status = .unavailable, .refusal = .{ .reason = .leader_unknown, .ran = .no, .message = "unavailable: the report could not be proposed — retry" } },
            error.NameHasNul => .{ .status = .bad_request, .refusal = .{ .reason = .malformed, .ran = .no, .message = machine.NAME_HAS_NUL } },
        };
    }

    // ── Register, invoke, delete, list (their wire as before) ───────────

    fn dispatchAction(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        switch (op) {
            .action_register, .action_invoke, .action_delete => if (std.mem.indexOfScalar(u8, req.key, 0) != null) {
                return sendActionResponse(shard, conn, req.header.request_id, .{ .err = .{ .code = .invalid_request, .message = "action name must not contain NUL" } });
            },
            else => {},
        }
        const result = switch (op) {
            .action_register => handleRegister(shard, req),
            .action_invoke => handleInvoke(shard, req),
            .action_list => handleList(shard, req),
            .action_list_runs => handleListRuns(shard, req),
            .action_delete => handleDelete(shard, req),
            else => CommandResult{ .err = .{ .code = .invalid_request, .message = "unknown action opcode" } },
        };
        defer switch (result) {
            .action_list_result => |r| shard.allocator.free(r.data),
            else => {},
        };
        if (result == .parked) {
            if (op == .action_register or op == .action_invoke) shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
            return shard.park(conn, req, result.parked, switch (op) {
                .action_register => answerRegister,
                .action_invoke => answerInvoke,
                else => answerOk,
            });
        }
        sendActionResponse(shard, conn, req.header.request_id, result);
    }

    fn answerRegister(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        _ = r.int(u32);
        shard.sendOkResponse(conn, request_id, r.slice() orelse "");
    }

    fn answerInvoke(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        sendActionResponse(shard, conn, request_id, .{ .action_invoked = .{ .run_id = r.slice() orelse "", .queue_position = @intCast(@min(storeOf(shard).family(machine.Runs.ID).count, std.math.maxInt(u32))) } });
    }

    fn answerOk(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        shard.sendOkResponse(conn, request_id, "");
    }

    /// `[action_type:u8][timeout_ms:u32][max_retries:u32][owner_len:u16][owner]`,
    /// every field defaulted. The timeout is the action's lease: 0 is the
    /// default, and one below the floor or past the ceiling is refused.
    fn handleRegister(shard: *Shard, req: Request) CommandResult {
        const name = req.key;
        if (name.len == 0) return .{ .err = .{ .code = .invalid_request, .message = "action name is required" } };
        if (name.len > action_ops.MAX_ACTION_NAME) return .{ .err = .{ .code = .invalid_request, .message = "action name too long" } };
        const ns = homeOf(req.namespace);
        if (getDef(shard, ns, name) == null and storeOf(shard).family(machine.Defs.ID).count >= MAX_ACTIONS)
            return .{ .err = .{ .code = .internal_error, .message = "action limit reached" } };
        const v = req.value;
        var timeout_ms: u64 = 0;
        var max_retries: u32 = 3;
        var owner: []const u8 = "";
        if (v.len >= 5) timeout_ms = std.mem.readInt(u32, v[1..5], .little);
        if (v.len >= 9) max_retries = std.mem.readInt(u32, v[5..9], .little);
        if (v.len >= 11) {
            const owner_len: usize = std.mem.readInt(u16, v[9..11], .little);
            if (v.len >= 11 + owner_len) owner = v[11 .. 11 + owner_len];
        }
        if (owner.len > ae.MAX_OWNER) return .{ .err = .{ .code = .invalid_request, .message = "owner too long" } };
        const lease_ms = if (timeout_ms == 0) DEFAULT_LEASE_MS else timeout_ms;
        if (lease_ms < ae.MIN_LEASE_MS or lease_ms > ae.MAX_LEASE_MS)
            return .{ .err = .{ .code = .invalid_request, .message = std.fmt.comptimePrint("bad request: an action's timeout is its lease: at least {d} ms and at most {d} ms, or 0 for {d} ms", .{ ae.MIN_LEASE_MS, ae.MAX_LEASE_MS, DEFAULT_LEASE_MS }) } };
        const proposed = propose(shard, .action_register, ae.Register.Value{ .ns = ns, .name = name, .owner = owner, .lease_ms = lease_ms, .max_attempts = std.math.clamp(max_retries, 1, 1000) }) catch |err|
            return .{ .err = persistence.failureError(err, "action not persisted") };
        return .{ .parked = proposed };
    }

    /// `[has_labels:u8]([labels_len:u16][labels])?[input...]`.
    fn handleInvoke(shard: *Shard, req: Request) CommandResult {
        const name = req.key;
        if (name.len == 0) return .{ .err = .{ .code = .invalid_request, .message = "action name is required" } };
        const ns = homeOf(req.namespace);
        if (getDef(shard, ns, name) == null) return .{ .err = .{ .code = .not_found, .message = "action not found" } };
        const parsed = parseInvokeValue(shard.allocator, req.value) catch |err| return .{ .err = switch (err) {
            error.OutOfMemory => .{ .code = .internal_error, .message = "out of memory" },
            error.Empty => .{ .code = .invalid_request, .message = "invoke value is empty: it starts with a has_labels byte" },
            error.BadFlag => .{ .code = .invalid_request, .message = "invoke has_labels must be 0 or 1" },
            error.Truncated => .{ .code = .invalid_request, .message = "invoke labels run past the end of the value" },
            error.LabelsNotObject => .{ .code = .invalid_request, .message = "invoke labels must be a JSON object" },
        } };
        if (parsed.input.len > ae.MAX_INPUT) return .{ .err = .{ .code = .invalid_request, .message = "action input too large" } };
        if (parsed.labels) |l| if (l.len > ae.MAX_LABELS) return .{ .err = .{ .code = .invalid_request, .message = "invoke labels too large" } };
        var id_buf: [32]u8 = undefined;
        const run_id = shard.run_id_gen.next(.action, shard.router.keyToPartitionNs(req.namespace, name), &id_buf);
        const proposed = propose(shard, .action_invoke, ae.Invoke.Value{ .ns = ns, .action = name, .run = run_id, .input = parsed.input, .labels = parsed.labels, .source = .direct, .caller_run = null, .caller_workflow = null }) catch |err|
            return .{ .err = persistence.failureError(err, "action not persisted") };
        return .{ .parked = proposed };
    }

    fn handleDelete(shard: *Shard, req: Request) CommandResult {
        const name = req.key;
        if (name.len == 0) return .{ .err = .{ .code = .invalid_request, .message = "action name is required" } };
        const ns = homeOf(req.namespace);
        // Deleting one that isn't there is ok.
        if (getDef(shard, ns, name) == null) return .{ .action_deleted = {} };
        const proposed = propose(shard, .action_delete, ae.Delete.Value{ .ns = ns, .name = name }) catch |err|
            return .{ .err = persistence.failureError(err, "action not persisted") };
        return .{ .parked = proposed };
    }

    /// `[count:u32]([key_len:u16][key][value_len:u32][type:u8][version:u32][enabled:u8])*[has_more:u8][cursor_len:u16]`:
    /// the actions in the namespace whose names start with the key.
    fn handleList(shard: *Shard, req: Request) CommandResult {
        var w: std.Io.Writer.Allocating = .init(shard.allocator);
        defer w.deinit();
        writeActionList(shard, homeOf(req.namespace), req.key, &w.writer) catch
            return .{ .err = .{ .code = .internal_error, .message = "action list serialization failed" } };
        const data = w.toOwnedSlice() catch return .{ .err = .{ .code = .internal_error, .message = "out of memory" } };
        return .{ .action_list_result = .{ .data = data, .cursor = null } };
    }

    fn writeActionList(shard: *Shard, ns: []const u8, prefix: []const u8, w: *std.Io.Writer) !void {
        var pb: [KEY_BUF]u8 = undefined;
        const p = storeKey(&pb, ns, prefix) orelse return error.WriteFailed;
        var count: u32 = 0;
        var it = readOf(shard).range(machine.Defs, p);
        while (try it.next()) |_| count += 1;
        try w.writeInt(u32, count, .little);
        it = readOf(shard).range(machine.Defs, p);
        while (try it.next()) |item| {
            const name = item.key[1 + ns.len ..];
            try w.writeInt(u16, @intCast(name.len), .little);
            try w.writeAll(name);
            try w.writeInt(u32, 6, .little);
            try w.writeByte(0); // user-hosted: the only type
            try w.writeInt(u32, item.value.v.version, .little);
            try w.writeByte(1);
        }
        try w.writeByte(0);
        try w.writeInt(u16, 0, .little);
    }

    /// The action walk's names: one shard's actions in the namespace that
    /// start with `prefix`, the request's key.
    fn localScanActions(ctx: *anyopaque, namespace: []const u8, prefix: []const u8, _: ?[]const u8, _: u32) dispatcher_mod.NameWalker.ScanResult {
        const shard: *Shard = @ptrCast(@alignCast(ctx));
        const ns = homeOf(namespace);
        const name_buf = dispatcher_mod.scanScratch(@intCast(storeOf(shard).family(machine.Defs.ID).count));
        var pb: [KEY_BUF]u8 = undefined;
        const p = storeKey(&pb, ns, prefix) orelse return .{ .items = name_buf[0..0], .next_cursor = null };
        var count: usize = 0;
        var it = readOf(shard).range(machine.Defs, p);
        while (it.next() catch null) |item| {
            if (count >= name_buf.len) break;
            name_buf[count] = item.key[1 + ns.len ..];
            count += 1;
        }
        return .{ .items = name_buf[0..count], .next_cursor = null };
    }

    /// One shard's runs of an action (or every action, for an empty key):
    ///   [count:u32] ([run_id_len:u16][run_id][action_name_len:u16][action_name]
    ///                [status:u8][created_at:i64][has_started:u8][started_at?:i64]
    ///                [has_completed:u8][completed_at?:i64])*
    fn handleListRuns(shard: *Shard, req: Request) CommandResult {
        const ns = homeOf(req.namespace);
        const limit: u32 = if (req.value.len >= 4) std.mem.readInt(u32, req.value[0..4], .little) else 100;
        var w: std.Io.Writer.Allocating = .init(shard.allocator);
        defer w.deinit();
        writeRuns(shard, ns, req.key, limit, &w.writer) catch
            return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        const data = w.toOwnedSlice() catch return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        return .{ .action_list_result = .{ .data = data, .cursor = null } };
    }

    fn writeRuns(shard: *Shard, ns: []const u8, action: []const u8, limit: u32, w: *std.Io.Writer) !void {
        var pb: [KEY_BUF]u8 = undefined;
        const p = storeKey(&pb, ns, "") orelse return error.WriteFailed;
        var count: u32 = 0;
        var it = readOf(shard).range(machine.Runs, p);
        while (try it.next()) |item| {
            if (count >= limit) break;
            if (action.len > 0 and !std.mem.eql(u8, item.value.v.action, action)) continue;
            count += 1;
        }
        try w.writeInt(u32, count, .little);
        var emitted: u32 = 0;
        it = readOf(shard).range(machine.Runs, p);
        while (try it.next()) |item| {
            if (emitted >= count) break;
            const run = item.value.v;
            if (action.len > 0 and !std.mem.eql(u8, run.action, action)) continue;
            emitted += 1;
            const id = item.key[1 + ns.len ..];
            try w.writeInt(u16, @intCast(id.len), .little);
            try w.writeAll(id);
            try w.writeInt(u16, @intCast(run.action.len), .little);
            try w.writeAll(run.action);
            try w.writeByte(@backingInt(statusOf(run)));
            try w.writeInt(i64, nsToMs(run.created_ns), .little);
            try writeOptionalMs(w, run.started_ns);
            try writeOptionalMs(w, run.completed_ns);
        }
    }

    fn writeOptionalMs(w: *std.Io.Writer, at_ns: u64) !void {
        if (at_ns == 0) return w.writeByte(0);
        try w.writeByte(1);
        try w.writeInt(i64, nsToMs(at_ns), .little);
    }

    /// A run's status as the wire says it.
    fn statusOf(run: Run) ActionRunStatus {
        return switch (run.status) {
            .ready => .pending,
            .leased => .running,
            .completed => .completed,
            .failed => .failed,
            .timed_out => .timed_out,
        };
    }

    // ── Status ──────────────────────────────────────────────────────────

    comptime {
        for (@typeInfo(ActionRunStatus).@"enum".field_names, @typeInfo(ActionRunStatus).@"enum".field_values) |name, value| {
            if (@backingInt(@field(action_ops.RunStatus, name)) != value) @compileError("action.status RunStatus." ++ name ++ " differs from the run's");
        }
    }

    fn runStatus(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(StatusResponse) {
        const self = shard.actions_handler;
        const run = getRun(shard, homeOf(req.namespace), req.key) orelse return error.NotFound;
        const buf = self.status_buf orelse blk: {
            const b = try self.allocator.alloc(u8, action_ops.MAX_OUTPUT + action_ops.MAX_ERROR + action_ops.MAX_WORKER_ID);
            self.status_buf = b;
            break :blk b;
        };
        var used: usize = 0;
        const output: ?[]const u8 = if (run.status == .completed) try copyInto(buf, &used, run.result, action_ops.MAX_OUTPUT) else null;
        const error_message: ?[]const u8 = if (run.@"error".len == 0) null else try copyInto(buf, &used, run.@"error", action_ops.MAX_ERROR);
        const worker_id: ?[]const u8 = if (run.holder.len == 0) null else try copyInto(buf, &used, run.holder, action_ops.MAX_WORKER_ID);
        return .{ .answer = .{
            .status = @fromBackingInt(@backingInt(statusOf(run))),
            .created_at_ms = nsToMs(run.created_ns),
            .started_at_ms = if (run.started_ns == 0) null else nsToMs(run.started_ns),
            .completed_at_ms = if (run.completed_ns == 0) null else nsToMs(run.completed_ns),
            .attempt = run.deliveries,
            .output = output,
            .error_message = error_message,
            .worker_id = worker_id,
        } };
    }

    fn copyInto(buf: []u8, used: *usize, bytes: []const u8, max: usize) Error![]const u8 {
        if (bytes.len > max) return error.StoredTooLong;
        @memcpy(buf[used.*..][0..bytes.len], bytes);
        defer used.* += bytes.len;
        return buf[used.*..][0..bytes.len];
    }

    // ── Await: on the worker's shard ────────────────────────────────────

    /// Take a task of one of the worker's actions: the request's, then the
    /// others it registered. An action this shard owns is delivered here;
    /// one another shard owns is asked of that shard, one at a time. With
    /// nothing anywhere the await waits, and runs again when woken.
    fn runAwait(shard: *Shard, conn: *Connection, req: Request, actions: schema.BoundedList(action_ops.ActionName, action_ops.MAX_AWAIT_ACTIONS, action_ops.MAX_AWAIT_ACTIONS * action_ops.ActionName.max_size).Value, block_ms: u64, request_key: ?[]const u8) Error!table_run.Outcome(AwaitResponse) {
        const self = shard.actions_handler;
        const ns = homeOf(req.namespace);
        const worker_id = req.key;
        if (worker_id.len > 0) if (shard.worker_handler.refusesWork(ns, worker_id)) |no| return .{ .refused = .{ .status = .bad_request, .refusal = no } };
        const labels: ?[]const u8 = if (worker_id.len > 0) if (shard.worker_handler.find(ns, worker_id)) |w| w.metadata_owned else null else null;

        var names: Names = .{};
        var it = actions.iterator();
        while (it.next() catch null) |name| names.add(name);
        if (names.len == 0) return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun("bad request: an await names at least one action") } };
        const primary = names.items[0];
        if (worker_id.len > 0) if (shard.worker_handler.find(ns, worker_id)) |w| for (w.processes.items) |p| {
            if (p.kind == .action) names.add(p.name_owned);
        };

        var remote: Names = .{};
        for (names.items[0..names.len]) |name| {
            if (ownerOf(shard, ns, name) != shard.id) {
                remote.add(name);
                continue;
            }
            switch (try self.deliverHere(shard, conn.replyTo(), req.header.request_id, ns, name, worker_id, labels, request_key)) {
                .proposed => return .wait,
                .again => |task| return .{ .answer = .{ .task = task } },
                .none => {},
            }
        }
        if (remote.len > 0) return askOwners(shard, conn, req, ns, primary, remote, worker_id, labels, request_key, block_ms);
        return self.waitForTask(shard, conn, req, ns, primary, worker_id, request_key, block_ms);
    }

    /// The names an await tries, each once, in order.
    const Names = struct {
        items: [action_ops.MAX_AWAIT_ACTIONS * 2][]const u8 = undefined,
        len: usize = 0,

        fn add(self: *Names, name: []const u8) void {
            if (name.len == 0 or self.len == self.items.len) return;
            for (self.items[0..self.len]) |n| if (std.mem.eql(u8, n, name)) return;
            self.items[self.len] = name;
            self.len += 1;
        }
    };

    fn waitForTask(self: *ActionsHandler, shard: *Shard, conn: *Connection, req: Request, ns: []const u8, primary: []const u8, worker_id: []const u8, request_key: ?[]const u8, block_ms: u64) Error!table_run.Outcome(AwaitResponse) {
        _ = self;
        if (block_ms == 0) return .{ .answer = .{ .task = null } };
        var key_buf: [waiter_pool_mod.MAX_WAITER_KEY]u8 = undefined;
        const key = AwaitKey.build(&key_buf, ns, primary, worker_id, request_key orelse "") orelse
            return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun("bad request: worker id and action name too long to wait on") } };
        if (!shard.waiter_pool.register(.{ .kind = .action_await, .reply_to = conn.replyTo(), .request_id = req.header.request_id, .key = key.bytes, .min_version = key.lengths, .timeout_ms = @intCast(@min(block_ms, waiter_pool_mod.MAX_BLOCK_MS)) })) {
            shard.refuseWaitersFull(conn, req.header.request_id);
            return .wait;
        }
        return .wait;
    }

    /// A woken await's waiter key: `[ns][action][worker][request_key]`,
    /// the lengths packed in its min_version.
    const AwaitKey = struct {
        bytes: []const u8,
        lengths: u64,

        fn build(buf: []u8, ns: []const u8, action: []const u8, worker: []const u8, request_key: []const u8) ?AwaitKey {
            const total = ns.len + action.len + worker.len + request_key.len;
            if (total > buf.len or ns.len > 0xFFFF or action.len > 0xFFFF or worker.len > 0xFFFF or request_key.len > 0xFFFF) return null;
            var off: usize = 0;
            for ([_][]const u8{ ns, action, worker, request_key }) |part| {
                @memcpy(buf[off..][0..part.len], part);
                off += part.len;
            }
            return .{ .bytes = buf[0..total], .lengths = (@as(u64, ns.len) << 48) | (@as(u64, action.len) << 32) | (@as(u64, worker.len) << 16) | request_key.len };
        }

        const Parts = struct { ns: []const u8, action: []const u8, worker: []const u8, request_key: []const u8 };

        fn parse(key: []const u8, lengths: u64) ?Parts {
            const lens = [4]usize{ @intCast(lengths >> 48), @intCast((lengths >> 32) & 0xFFFF), @intCast((lengths >> 16) & 0xFFFF), @intCast(lengths & 0xFFFF) };
            if (lens[0] + lens[1] + lens[2] + lens[3] != key.len) return null;
            const a = lens[0];
            const b = a + lens[1];
            const c = b + lens[2];
            return .{ .ns = key[0..a], .action = key[a..b], .worker = key[b..c], .request_key = key[c..] };
        }
    };

    // ── Asking other shards ─────────────────────────────────────────────

    /// What an await asking other owners keeps between asks.
    const Ask = struct {
        req_bytes: []u8,
        ns: []u8,
        primary: []u8,
        worker: []u8,
        labels: ?[]u8,
        request_key: ?[]u8,
        block_ms: u64,
        /// The names left to ask, each `[len:u16][name]`.
        left: []u8,
        left_off: usize = 0,

        fn free(state: *anyopaque, allocator: Allocator) void {
            const a: *Ask = @ptrCast(@alignCast(state));
            allocator.free(a.req_bytes);
            allocator.free(a.ns);
            allocator.free(a.primary);
            allocator.free(a.worker);
            if (a.labels) |l| allocator.free(l);
            if (a.request_key) |k| allocator.free(k);
            allocator.free(a.left);
            allocator.destroy(a);
        }

        fn nextName(a: *Ask) ?[]const u8 {
            if (a.left_off + 2 > a.left.len) return null;
            const n: usize = std.mem.readInt(u16, a.left[a.left_off..][0..2], .little);
            defer a.left_off += 2 + n;
            return a.left[a.left_off + 2 ..][0..n];
        }

        const op: gather_mod.Op = .{ .merge = merge, .deinit = free };

        /// The owner answered: its task is the await's; none asks the next
        /// owner, or waits once every owner said none.
        fn merge(shard: *Shard, conn: *Connection, request_id: u64, state: *anyopaque, bodies: []const []const u8) anyerror!void {
            const a: *Ask = @ptrCast(@alignCast(state));
            var diag: schema.Diagnostic = .{};
            const got = try DeliverResponse.decodeAll(bodies[0], &diag);
            if (got.task) |task| return table_run.answer(AwaitResponse, shard, conn, request_id, .{ .task = task });
            const req = try Request.parse(a.req_bytes);
            if (a.nextName()) |name| {
                // The state moves to the next ask, which frees it.
                const next = try a.clone(shard.allocator);
                return startAsk(shard, conn, req, name, next);
            }
            const outcome = try shard.actions_handler.waitForTask(shard, conn, req, a.ns, a.primary, a.worker, a.request_key, a.block_ms);
            switch (outcome) {
                .answer => |v| table_run.answer(AwaitResponse, shard, conn, request_id, v),
                .wait => conn.response_deferred = true,
                .refused => |r| shard.sendRefusal(conn, request_id, r.status, r.refusal),
                .park => unreachable,
            }
        }

        fn clone(a: *const Ask, allocator: Allocator) !*Ask {
            const c = try allocator.create(Ask);
            errdefer allocator.destroy(c);
            c.* = .{
                .req_bytes = try allocator.dupe(u8, a.req_bytes),
                .ns = try allocator.dupe(u8, a.ns),
                .primary = try allocator.dupe(u8, a.primary),
                .worker = try allocator.dupe(u8, a.worker),
                .labels = if (a.labels) |l| try allocator.dupe(u8, l) else null,
                .request_key = if (a.request_key) |k| try allocator.dupe(u8, k) else null,
                .block_ms = a.block_ms,
                .left = try allocator.dupe(u8, a.left),
                .left_off = a.left_off,
            };
            return c;
        }
    };

    fn askOwners(shard: *Shard, conn: *Connection, req: Request, ns: []const u8, primary: []const u8, remote: Names, worker_id: []const u8, labels: ?[]const u8, request_key: ?[]const u8, block_ms: u64) Error!table_run.Outcome(AwaitResponse) {
        const a = buildAsk(shard.allocator, req, ns, primary, remote, worker_id, labels, request_key, block_ms) catch return error.OutOfMemory;
        const first = a.nextName().?;
        startAsk(shard, conn, req, first, a) catch |err| return switch (err) {
            error.GatherBusy => .{ .refused = .{ .status = .overloaded, .refusal = .{ .reason = .shard_busy, .ran = .no, .message = "overloaded: too many requests across shards are under way; this await took nothing — retry" } } },
            else => error.OutOfMemory,
        };
        return .wait;
    }

    fn buildAsk(allocator: Allocator, req: Request, ns: []const u8, primary: []const u8, remote: Names, worker_id: []const u8, labels: ?[]const u8, request_key: ?[]const u8, block_ms: u64) !*Ask {
        var left: std.ArrayListUnmanaged(u8) = .empty;
        defer left.deinit(allocator);
        for (remote.items[0..remote.len]) |name| {
            var len: [2]u8 = undefined;
            std.mem.writeInt(u16, &len, @intCast(name.len), .little);
            try left.appendSlice(allocator, &len);
            try left.appendSlice(allocator, name);
        }
        const a = try allocator.create(Ask);
        errdefer allocator.destroy(a);
        a.* = .{
            .req_bytes = try Shard.serializeRequest(allocator, req),
            .ns = try allocator.dupe(u8, ns),
            .primary = try allocator.dupe(u8, primary),
            .worker = try allocator.dupe(u8, worker_id),
            .labels = if (labels) |l| try allocator.dupe(u8, l) else null,
            .request_key = if (request_key) |k| try allocator.dupe(u8, k) else null,
            .block_ms = block_ms,
            .left = try left.toOwnedSlice(allocator),
        };
        return a;
    }

    /// Ask the owner of `name` for a task of it. `a` is the gather's from
    /// here, freed however it ends.
    fn startAsk(shard: *Shard, conn: *Connection, req: Request, name: []const u8, a: *Ask) !void {
        const body_size = DeliverRequest.encodedSize(.{ .holder = a.worker, .labels = a.labels, .request_key = a.request_key }) catch {
            Ask.free(a, shard.allocator);
            return error.OutOfMemory;
        };
        const body = shard.allocator.alloc(u8, body_size) catch {
            Ask.free(a, shard.allocator);
            return error.OutOfMemory;
        };
        defer shard.allocator.free(body);
        _ = DeliverRequest.encodeInto(body, .{ .holder = a.worker, .labels = a.labels, .request_key = a.request_key }) catch unreachable;
        const parts = [_]gather_mod.Part{.{ .owner = ownerOf(shard, a.ns, name), .op = @backingInt(OpCode.action_deliver), .key = name, .body = body }};
        try shard.startGather(conn, req, &parts, &Ask.op, a);
        conn.response_deferred = true;
    }

    // ── Deliver: on the action's shard ──────────────────────────────────

    /// Another shard's await asking for a task of this shard's action.
    fn runDeliver(shard: *Shard, conn: *Connection, req: Request, holder: []const u8, labels: ?[]const u8, request_key: ?[]const u8) Error!table_run.Outcome(DeliverResponse) {
        return switch (try shard.actions_handler.deliverHere(shard, conn.replyTo(), req.header.request_id, homeOf(req.namespace), req.key, holder, labels, request_key)) {
            .proposed => .wait,
            .again => |task| .{ .answer = .{ .task = task } },
            .none => .{ .answer = .{ .task = null } },
        };
    }

    const Delivered = union(enum) {
        /// A deliver was proposed: its await is answered when it applies.
        proposed,
        /// The request key's delivery, still held: answered again as is.
        again: Task,
        none,
    };

    /// The next ready run of `action` the worker may take, delivered to it:
    /// earliest due first, skipping runs already being delivered and runs
    /// whose labels the worker lacks. Only a leader whose term's entries
    /// have applied chooses: before that, a run delivered in the last term
    /// may still read as ready here.
    fn deliverHere(self: *ActionsHandler, shard: *Shard, reply_to: ReplyTo, request_id: u64, ns: []const u8, action: []const u8, holder: []const u8, labels: ?[]const u8, request_key: ?[]const u8) Error!Delivered {
        if (!shard.raft_node.leaderReady()) return .none;
        if (request_key) |rk| if (self.again(shard, holder, rk)) |task| return .{ .again = task };
        const def = getDef(shard, ns, action) orelse return .none;
        const now = wallNs();
        var pb: [KEY_BUF]u8 = undefined;
        var rb: [machine.MAX_KEY_LEN]u8 = undefined;
        const prefix = storeKey(&pb, ns, machine.readyPrefix(&rb, action)) orelse return .none;
        var it = readOf(shard).range(machine.Ready, prefix);
        var looked: usize = 0;
        while (it.next() catch null) |item| : (looked += 1) {
            if (looked == MAX_LOOKED_AT) break;
            const tail = item.key[prefix.len..];
            if (tail.len < 8) continue;
            const due = std.mem.readInt(u64, tail[0..8], .big);
            if (due > now) break;
            const run_id = tail[8..];
            var kb: [KEY_BUF]u8 = undefined;
            const run_key = storeKey(&kb, ns, run_id) orelse continue;
            if (self.reserved.contains(run_key)) continue;
            const run = getRun(shard, ns, run_id) orelse continue;
            if (run.labels) |required| {
                const have = labels orelse continue;
                if (!labelsMatch(self.allocator, required, have)) continue;
            }
            const proposed = propose(shard, .action_deliver, ae.Deliver.Value{ .ns = ns, .action = action, .run = run_id, .due_ns = due, .holder = holder, .request_key = request_key, .lease_ms = def.lease_ms }) catch |err| {
                if (!shard.quietHalted(err)) log.warn("shard {d}: run {s} not delivered: {s}", .{ shard.id, run_id, @errorName(err) });
                return .none;
            };
            const owned_key = try self.allocator.dupe(u8, run_key);
            self.reserved.put(self.allocator, owned_key, {}) catch {
                self.allocator.free(owned_key);
                return error.OutOfMemory;
            };
            const kept: ?[]u8 = if (request_key) |rk| try rememberKey(self.allocator, holder, rk) else null;
            self.pending.append(self.allocator, .{ .index = proposed.index, .term = proposed.term, .reply_to = reply_to, .request_id = request_id, .remember = kept }) catch {
                if (kept) |r| self.allocator.free(r);
                return error.OutOfMemory;
            };
            return .proposed;
        }
        return .none;
    }

    fn rememberKey(allocator: Allocator, holder: []const u8, request_key: []const u8) ![]u8 {
        const k = try allocator.alloc(u8, holder.len + 1 + request_key.len);
        @memcpy(k[0..holder.len], holder);
        k[holder.len] = 0;
        @memcpy(k[holder.len + 1 ..], request_key);
        return k;
    }

    /// The delivery a retried await's request key got, if its holder holds
    /// it still.
    fn again(self: *ActionsHandler, shard: *Shard, holder: []const u8, request_key: []const u8) ?Task {
        var kb: [action_ops.MAX_WORKER_ID + 1 + action_ops.MAX_REQUEST_KEY]u8 = undefined;
        if (holder.len + 1 + request_key.len > kb.len) return null;
        @memcpy(kb[0..holder.len], holder);
        kb[holder.len] = 0;
        @memcpy(kb[holder.len + 1 ..][0..request_key.len], request_key);
        const r = self.remembered.get(kb[0 .. holder.len + 1 + request_key.len]) orelse return null;
        const run = getRun(shard, r.ns, r.run) orelse return null;
        if (run.status != .leased or run.token.term != r.token.term or run.token.index != r.token.index) return null;
        return taskOf(r.run, run);
    }

    /// The task a delivered run is.
    fn taskOf(run_id: []const u8, run: Run) Task {
        return .{
            .run = run_id,
            .deliveries = run.deliveries,
            .term = run.token.term,
            .index = run.token.index,
            .action = run.action,
            .created_at_ms = nsToMs(run.created_ns),
            .attempt = run.failures +| 1,
            .lease_ms = run.lease_ms,
            .caller_run = run.caller_run,
            .caller_workflow = run.caller_workflow,
            .input = run.input,
        };
    }

    /// A deliver applied: answer the await waiting for it, and free its run
    /// for choosing again either way.
    fn deliverApplied(self: *ActionsHandler, shard: *Shard, e: *const Entry, result: *const Result) void {
        var diag: schema.Diagnostic = .{};
        const p = ae.Deliver.decodeAll(e.payload, &diag) catch return;
        var kb: [KEY_BUF]u8 = undefined;
        if (storeKey(&kb, p.ns, p.run)) |k| if (self.reserved.fetchRemove(k)) |kv| self.allocator.free(kv.key);
        const at = for (self.pending.items, 0..) |pd, i| {
            if (pd.index == e.header.index and pd.term == e.header.term) break i;
        } else return;
        const pd = self.pending.swapRemove(at);
        defer if (pd.remember) |r| self.allocator.free(r);
        const task: ?Task = if (result.refused() != null) null else if (getRun(shard, p.ns, p.run)) |run| taskOf(p.run, run) else null;
        if (task) |t| if (pd.remember) |r| self.remember(r, p.ns, p.run, t, getRun(shard, p.ns, p.run).?.expiry_ns);
        deliverAnswer(shard, pd.reply_to, pd.request_id, task);
    }

    fn remember(self: *ActionsHandler, key: []const u8, ns: []const u8, run_id: []const u8, task: Task, expiry_ns: u64) void {
        self.pruneRemembered();
        const k = self.allocator.dupe(u8, key) catch return;
        const ns_owned = self.allocator.dupe(u8, ns) catch {
            self.allocator.free(k);
            return;
        };
        const run_owned = self.allocator.dupe(u8, run_id) catch {
            self.allocator.free(k);
            self.allocator.free(ns_owned);
            return;
        };
        const got = self.remembered.getOrPut(self.allocator, k) catch {
            self.allocator.free(k);
            self.allocator.free(ns_owned);
            self.allocator.free(run_owned);
            return;
        };
        if (got.found_existing) {
            self.allocator.free(k);
            self.allocator.free(got.value_ptr.ns);
            self.allocator.free(got.value_ptr.run);
        }
        got.value_ptr.* = .{ .ns = ns_owned, .run = run_owned, .token = .{ .term = task.term, .index = task.index }, .expiry_ns = expiry_ns };
    }

    /// Drop remembered deliveries whose lease has run out, once there are
    /// many: a request key is kept for one lease.
    fn pruneRemembered(self: *ActionsHandler) void {
        if (self.remembered.count() < 4096) return;
        const now = wallNs();
        var it = self.remembered.iterator();
        var gone: std.ArrayListUnmanaged([]const u8) = .empty;
        defer gone.deinit(self.allocator);
        while (it.next()) |e| if (e.value_ptr.expiry_ns < now) gone.append(self.allocator, e.key_ptr.*) catch break;
        for (gone.items) |k| if (self.remembered.fetchRemove(k)) |kv| {
            self.allocator.free(kv.key);
            self.allocator.free(kv.value.ns);
            self.allocator.free(kv.value.run);
        };
    }

    /// An await or ask answered with its task, or none.
    fn deliverAnswer(shard: *Shard, reply_to: ReplyTo, request_id: u64, task: ?Task) void {
        const buf = shard.allocator.create([@sizeOf(proto.ResponseHeader) + AwaitResponse.max_size]u8) catch
            return shard.deliverDeferredRefusal(reply_to, request_id, .internal_error, .{ .reason = .answer_lost, .ran = .yes, .message = "internal error: out of memory for the task; it is held until its lease runs out" });
        defer shard.allocator.destroy(buf);
        const bytes = table_run.encodeAnswer(AwaitResponse, buf, request_id, .{ .task = task }) orelse
            return shard.deliverDeferredRefusal(reply_to, request_id, .internal_error, .{ .reason = .answer_lost, .ran = .yes, .message = "internal error: the task didn't fit its answer; it is held until its lease runs out" });
        shard.deliverDeferred(reply_to, bytes);
    }

    // ── Waking ──────────────────────────────────────────────────────────

    /// Runs may be ready: wake this shard's awaits, and tell the other
    /// shards, whose workers may take them. After an apply pass in which an
    /// invoke or a retry applied on the leader.
    pub fn wakeWorkers(shard: *Shard) void {
        wakeLocal(shard);
        if (shard.peer_mailboxes) |mailboxes| {
            for (mailboxes, 0..) |mb, i| {
                if (i == shard.id) continue;
                mb.wake.set(.action_invoked);
            }
        }
    }

    /// Wake this shard's awaits: each that may find a task runs again, as
    /// it first ran, with the wait it has left.
    pub fn wakeLocal(shard: *Shard) void {
        shard.waiter_pool.notifyAny(.action_await, resolveAwait, @ptrCast(shard));
        const self = shard.actions_handler;
        if (self.reruns.items.len == 0) return;
        const reruns = self.reruns.toOwnedSlice(self.allocator) catch return;
        defer self.allocator.free(reruns);
        for (reruns) |r| {
            defer self.allocator.free(r.bytes);
            shard.runHeld(r.bytes, r.reply_to, r.request_id);
        }
    }

    /// A waiting await that may find a task now: it leaves the pool and
    /// runs again after the wake. False leaves it waiting.
    fn resolveAwait(waiter: *Waiter, ctx: *anyopaque) bool {
        const shard: *Shard = @ptrCast(@alignCast(ctx));
        const self = shard.actions_handler;
        const parts = AwaitKey.parse(waiter.key(), waiter.min_version) orelse return false;
        if (parts.worker.len > 0) if (shard.worker_handler.refusesWork(parts.ns, parts.worker)) |no| {
            shard.deliverDeferredRefusal(waiter.reply_to, waiter.request_id, .bad_request, no);
            return true;
        };
        if (!self.mayFind(shard, parts)) return false;
        const now = @import("stdx").time.monotonicMs();
        const left = waiter.expires_at_ms -| now;
        if (left == 0) return false;
        const bytes = rerunBytes(shard.allocator, waiter.request_id, parts, left) catch return false;
        self.reruns.append(self.allocator, .{ .reply_to = waiter.reply_to, .request_id = waiter.request_id, .bytes = bytes }) catch {
            shard.allocator.free(bytes);
            return false;
        };
        return true;
    }

    /// Whether a woken await may find a task: one of its actions is another
    /// shard's (which the wake came from), or has a ready run due here.
    fn mayFind(self: *ActionsHandler, shard: *Shard, parts: AwaitKey.Parts) bool {
        _ = self;
        if (mayFindOf(shard, parts.ns, parts.action)) return true;
        if (parts.worker.len > 0) if (shard.worker_handler.find(parts.ns, parts.worker)) |w| for (w.processes.items) |p| {
            if (p.kind == .action and mayFindOf(shard, parts.ns, p.name_owned)) return true;
        };
        return false;
    }

    fn mayFindOf(shard: *Shard, ns: []const u8, action: []const u8) bool {
        if (ownerOf(shard, ns, action) != shard.id) return true;
        var pb: [KEY_BUF]u8 = undefined;
        var rb: [machine.MAX_KEY_LEN]u8 = undefined;
        const prefix = storeKey(&pb, ns, machine.readyPrefix(&rb, action)) orelse return false;
        var it = readOf(shard).range(machine.Ready, prefix);
        const first = (it.next() catch return false) orelse return false;
        const tail = first.key[prefix.len..];
        return tail.len >= 8 and std.mem.readInt(u64, tail[0..8], .big) <= wallNs();
    }

    /// The await a waiter was, as a request, waiting `left_ms` at most.
    fn rerunBytes(allocator: Allocator, request_id: u64, parts: AwaitKey.Parts, left_ms: u64) ![]u8 {
        var lb: [action_ops.ActionName.max_size]u8 = undefined;
        var lw: schema.Writer = .{ .buf = &lb };
        try action_ops.ActionName.encode(&lw, parts.action);
        const body: AwaitRequest.Value = .{ .actions = .{ .bytes = lb[0..lw.pos], .count = 1 }, .block_ms = @min(left_ms, action_ops.MAX_AWAIT_MS), .request_key = if (parts.request_key.len == 0) null else parts.request_key };
        const value = try allocator.alloc(u8, try AwaitRequest.encodedSize(body));
        defer allocator.free(value);
        _ = try AwaitRequest.encodeInto(value, body);
        const req: Request = .{ .header = proto.RequestHeader.init(.action_await, request_id), .namespace = parts.ns, .key = parts.worker, .value = value, .options = "" };
        return Shard.serializeRequest(allocator, req);
    }

    // ── Reports: complete, fail, touch ──────────────────────────────────

    /// The run a report names, if in scope (namespace and action), and the
    /// report's delivery its live one. A resend of the report that ended
    /// the delivery, and one for a delivery since moved on, are told apart
    /// by the machine, which answers both as it did.
    fn reported(shard: *Shard, req: Request, action: []const u8) Error!Run {
        if (!shard.raft_node.leaderReady()) return error.NotLeading;
        const run = getRun(shard, homeOf(req.namespace), req.key) orelse return error.NotFound;
        if (!std.mem.eql(u8, run.action, action)) return error.NotFound;
        return run;
    }

    fn runComplete(shard: *Shard, _: *Connection, req: Request, action: []const u8, worker: []const u8, deliveries: u32, term: u64, index: u64, outcome: []const u8, result: []const u8) Error!table_run.Outcome(EmptyResponse) {
        const run = try reported(shard, req, action);
        const proposed = propose(shard, .action_settle, ae.Settle.Value{ .ns = homeOf(req.namespace), .action = action, .run = req.key, .deliveries = deliveries, .token = .{ .term = term, .index = index }, .expiry_ns = run.expiry_ns, .outcome = outcome, .result = result }) catch |err|
            return .{ .refused = .{ .status = persistence.failureStatus(err), .refusal = persistence.failureRefusal(err, "complete not persisted") } };
        recordReport(shard, req.namespace, worker, action, false);
        return .{ .park = .{ .proposed = proposed, .answer = answerEmpty } };
    }

    fn runFail(shard: *Shard, _: *Connection, req: Request, action: []const u8, worker: []const u8, deliveries: u32, term: u64, index: u64, retry: bool, abandoned: bool, delay_ms: u64, err_text: []const u8) Error!table_run.Outcome(EmptyResponse) {
        const run = try reported(shard, req, action);
        const proposed = propose(shard, .action_release, ae.Release.Value{ .ns = homeOf(req.namespace), .action = action, .run = req.key, .deliveries = deliveries, .token = .{ .term = term, .index = index }, .expiry_ns = run.expiry_ns, .abandoned = abandoned, .retry = retry, .delay_ms = delay_ms, .@"error" = err_text }) catch |err|
            return .{ .refused = .{ .status = persistence.failureStatus(err), .refusal = persistence.failureRefusal(err, "fail not persisted") } };
        if (!abandoned) recordReport(shard, req.namespace, worker, action, true);
        return .{ .park = .{ .proposed = proposed, .answer = answerEmpty } };
    }

    /// Keep a delivery: answered with the lease as it stands while more
    /// than half of it is left, or while an extend for it is in flight;
    /// otherwise an extend is proposed and answered when it applies.
    fn runTouch(shard: *Shard, _: *Connection, req: Request, action: []const u8, worker: []const u8, deliveries: u32, term: u64, index: u64) Error!table_run.Outcome(TouchResponse) {
        _ = worker;
        const self = shard.actions_handler;
        const run = try reported(shard, req, action);
        if (run.status != .leased or run.deliveries != deliveries or run.token.term != term or run.token.index != index) return error.Stale;
        var kb: [KEY_BUF]u8 = undefined;
        const run_key = storeKey(&kb, homeOf(req.namespace), req.key) orelse return error.NotFound;
        const half = (run.lease_ms *| std.time.ns_per_ms) / 2;
        if (run.expiry_ns -| wallNs() > half or self.extending.contains(run_key)) return .{ .answer = .{ .expires_at_ms = nsToMs(run.expiry_ns) } };
        const proposed = propose(shard, .action_extend, ae.Extend.Value{ .ns = homeOf(req.namespace), .action = action, .run = req.key, .token = .{ .term = term, .index = index }, .expiry_ns = run.expiry_ns, .held_since_ns = run.delivered_ns, .lease_ms = run.lease_ms }) catch |err|
            return .{ .refused = .{ .status = persistence.failureStatus(err), .refusal = persistence.failureRefusal(err, "touch not persisted") } };
        const owned = try self.allocator.dupe(u8, run_key);
        self.extending.put(self.allocator, owned, {}) catch {
            self.allocator.free(owned);
            return error.OutOfMemory;
        };
        return .{ .park = .{ .proposed = proposed, .answer = answerTouch } };
    }

    fn answerEmpty(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        table_run.answer(EmptyResponse, shard, conn, request_id, .{});
    }

    fn answerTouch(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        table_run.answer(TouchResponse, shard, conn, request_id, .{ .expires_at_ms = nsToMs(r.int(u64) orelse 0) });
    }

    /// A worker's report counts toward its stats where it is registered,
    /// and only there: another shard's registry is that shard's to change.
    fn recordReport(shard: *Shard, namespace: []const u8, worker: []const u8, action: []const u8, failed: bool) void {
        if (worker.len == 0) return;
        if (ownerOf(shard, homeOf(namespace), worker) != shard.id) return;
        if (failed) shard.worker_handler.recordFailure(namespace, worker, action) else shard.worker_handler.recordCompletion(namespace, worker, action);
    }

    // ── After an entry applies ──────────────────────────────────────────

    /// What the leader does once an action entry applies: answer the await
    /// its deliver was for, wake awaits for a run newly ready, let its
    /// extend or expire be proposed again, and tell a workflow on another
    /// shard that a run it waits on finished. Every replica runs it; only a
    /// leader holds anything it acts on.
    pub fn afterApply(self: *ActionsHandler, shard: *Shard, e: *const Entry, result: *const Result) void {
        const etype: EntryType = @fromBackingInt(@intCast(e.header.entry_type));
        const leads = shard.raft_node.role == .leader;
        switch (etype) {
            .action_deliver => if (leads) self.deliverApplied(shard, e, result),
            .action_invoke => if (leads and result.refused() == null) {
                shard.wake_workers = true;
            },
            .action_extend => {
                var diag: schema.Diagnostic = .{};
                const p = ae.Extend.decodeAll(e.payload, &diag) catch return;
                var kb: [KEY_BUF]u8 = undefined;
                if (storeKey(&kb, p.ns, p.run)) |k| if (self.extending.fetchRemove(k)) |kv| self.allocator.free(kv.key);
            },
            .action_expire => {
                if (self.expiring) |i| if (i == e.header.index) {
                    self.expiring = null;
                };
                if (leads) shard.wake_workers = true;
                if (leads) self.tellWorkflowsOfExpire(shard, e);
            },
            .action_settle, .action_release => if (leads and result.refused() == null) {
                if (etype == .action_release) shard.wake_workers = true;
                var diag: schema.Diagnostic = .{};
                const ns, const run_id = if (etype == .action_settle) blk: {
                    const p = ae.Settle.decodeAll(e.payload, &diag) catch return;
                    break :blk .{ p.ns, p.run };
                } else blk: {
                    const p = ae.Release.decodeAll(e.payload, &diag) catch return;
                    break :blk .{ p.ns, p.run };
                };
                self.tellWorkflow(shard, ns, run_id);
            },
            else => {},
        }
    }

    fn tellWorkflowsOfExpire(self: *ActionsHandler, shard: *Shard, e: *const Entry) void {
        var diag: schema.Diagnostic = .{};
        const p = ae.Expire.decodeAll(e.payload, &diag) catch return;
        var it = p.items.iterator();
        while (it.next() catch return) |item| self.tellWorkflow(shard, item.ns, item.run);
    }

    /// A run a workflow waits on finished: its result goes to the
    /// workflow's shard, unless that is this one, where the workflow reads
    /// the store.
    fn tellWorkflow(self: *ActionsHandler, shard: *Shard, ns: []const u8, run_id: []const u8) void {
        const run = getRun(shard, ns, run_id) orelse return;
        if (!finishedStatus(run.status)) return;
        const caller = run.caller_run orelse return;
        const target = shard.router.partitionToShard(run_id_mod.extractPartition(caller) orelse return);
        if (target == shard.id) return;
        self.sendFinished(shard, target, run_id, run);
    }

    fn finishedStatus(s: machine.Status) bool {
        return switch (s) {
            .completed, .failed, .timed_out => true,
            .ready, .leased => false,
        };
    }

    /// `[run_len:u16][run][status:u8][outcome_len:u16][outcome][output]`.
    fn sendFinished(self: *ActionsHandler, shard: *Shard, target: u16, run_id: []const u8, run: Run) void {
        const outcome = if (run.status == .completed) run.outcome else "";
        const output = if (run.status == .completed) run.result else "";
        const bytes = self.allocator.alloc(u8, 2 + run_id.len + 1 + 2 + outcome.len + output.len) catch return;
        std.mem.writeInt(u16, bytes[0..2], @intCast(run_id.len), .little);
        @memcpy(bytes[2..][0..run_id.len], run_id);
        bytes[2 + run_id.len] = @backingInt(statusOf(run));
        std.mem.writeInt(u16, bytes[3 + run_id.len ..][0..2], @intCast(outcome.len), .little);
        @memcpy(bytes[5 + run_id.len ..][0..outcome.len], outcome);
        @memcpy(bytes[5 + run_id.len + outcome.len ..], output);
        self.send(shard, target, .action_finished, bytes);
    }

    fn send(self: *ActionsHandler, shard: *Shard, target: u16, tag: @import("../node/inbox.zig").Tag, bytes: []u8) void {
        const mailboxes = shard.peer_mailboxes orelse return self.allocator.free(bytes);
        if (target >= mailboxes.len) return self.allocator.free(bytes);
        if (mailboxes[target].inbox.send(.{ .tag = tag, .src_shard = @intCast(shard.id), .payload_len = @intCast(bytes.len), .payload_ptr = bytes.ptr })) return;
        self.unsent.append(self.allocator, .{ .target = target, .tag = tag, .bytes = bytes }) catch self.allocator.free(bytes);
    }

    /// A run's own shard telling this one how a run a workflow here waits
    /// on ended. Kept until the workflow takes it.
    pub fn finishedFromInbox(self: *ActionsHandler, bytes: []const u8) void {
        if (bytes.len < 5) return;
        const rid_len: usize = std.mem.readInt(u16, bytes[0..2], .little);
        if (5 + rid_len > bytes.len) return;
        const run_id = bytes[2..][0..rid_len];
        const status = std.enums.fromInt(ActionRunStatus, bytes[2 + rid_len]) orelse return;
        const outcome_len: usize = std.mem.readInt(u16, bytes[3 + rid_len ..][0..2], .little);
        if (5 + rid_len + outcome_len > bytes.len) return;
        const outcome = bytes[5 + rid_len ..][0..outcome_len];
        const output = bytes[5 + rid_len + outcome_len ..];
        if (self.finished.contains(run_id)) return;
        const k = self.allocator.dupe(u8, run_id) catch return;
        const o = self.allocator.dupe(u8, outcome) catch return self.allocator.free(k);
        const out = self.allocator.dupe(u8, output) catch {
            self.allocator.free(k);
            return self.allocator.free(o);
        };
        self.finished.put(self.allocator, k, .{ .status = status, .outcome = o, .output = out }) catch {
            self.allocator.free(k);
            self.allocator.free(o);
            self.allocator.free(out);
        };
    }

    /// A workflow here that has heard nothing of a run asks its shard; the
    /// answer comes as `action_finished` if the run has finished.
    pub fn askResult(self: *ActionsHandler, shard: *Shard, target: u16, run_id: []const u8) void {
        const bytes = self.allocator.dupe(u8, run_id) catch return;
        self.send(shard, target, .action_result_ask, bytes);
    }

    pub fn resultAskFromInbox(self: *ActionsHandler, shard: *Shard, from: u16, run_id: []const u8) void {
        // The run's namespace isn't in the ask: the caller's run id names a
        // run in one namespace, and run ids are unique on a shard.
        var it = readOf(shard).range(machine.Runs, "");
        while (it.next() catch null) |item| {
            const ns_len = item.key[0];
            if (!std.mem.eql(u8, item.key[1 + ns_len ..], run_id)) continue;
            if (finishedStatus(item.value.v.status)) self.sendFinished(shard, from, run_id, item.value.v);
            return;
        }
    }

    // ── For workflows ───────────────────────────────────────────────────

    /// How a run ended, as a workflow reads it.
    pub const InternalRunResult = struct {
        status: ActionRunStatus,
        output: ?[]const u8,
        /// "success" for a completed run that named none.
        outcome: ?[]const u8 = null,
    };

    /// A run on this shard, read from the store.
    pub fn getRunResult(shard: *Shard, ns: []const u8, run_id: []const u8) ?InternalRunResult {
        const run = getRun(shard, homeOf(ns), run_id) orelse return null;
        return .{
            .status = statusOf(run),
            .output = if (run.status == .completed) run.result else null,
            .outcome = if (run.status == .completed) (if (run.outcome.len == 0) "success" else run.outcome) else null,
        };
    }

    /// A run on another shard, as that shard last told this one; taken, so
    /// the workflow resumes once.
    pub fn takeFinished(self: *ActionsHandler, run_id: []const u8) ?Finished {
        const kv = self.finished.fetchRemove(run_id) orelse return null;
        self.allocator.free(kv.key);
        return kv.value;
    }

    pub fn freeFinished(self: *ActionsHandler, f: Finished) void {
        self.allocator.free(f.outcome);
        self.allocator.free(f.output);
    }

    /// Whether action `name` exists in `ns` here.
    pub fn actionExists(shard: *Shard, ns: []const u8, name: []const u8) bool {
        return getDef(shard, homeOf(ns), name) != null;
    }

    /// An invoke a workflow proposed: the run's id and the entry's index.
    pub const Invoked = struct { id: []const u8, index: u64 };

    /// Invoke an action for a workflow run on this shard: the run exists
    /// once the invoke applies. Null if the action doesn't exist or the
    /// invoke could not be proposed.
    pub fn invokeByName(shard: *Shard, namespace: []const u8, action_name: []const u8, input: ?[]const u8, caller_run_id: ?[]const u8, caller_workflow_name: ?[]const u8, id_buf: *[32]u8) ?Invoked {
        const ns = homeOf(namespace);
        if (!actionExists(shard, ns, action_name)) return null;
        const run_id = shard.run_id_gen.next(.action, shard.router.keyToPartitionNs(namespace, action_name), id_buf);
        const index = proposeInvoke(shard, ns, action_name, run_id, input orelse "", caller_run_id, caller_workflow_name) orelse return null;
        return .{ .id = run_id, .index = index };
    }

    fn proposeInvoke(shard: *Shard, ns: []const u8, action: []const u8, run_id: []const u8, input: []const u8, caller_run: ?[]const u8, caller_workflow: ?[]const u8) ?u64 {
        const proposed = propose(shard, .action_invoke, ae.Invoke.Value{ .ns = ns, .action = action, .run = run_id, .input = input, .labels = null, .source = if (caller_run != null) .workflow else .direct, .caller_run = caller_run, .caller_workflow = caller_workflow }) catch |err| {
            if (!shard.quietHalted(err)) log.err("shard {d}: action run {s} not started: {s}", .{ shard.id, run_id, @errorName(err) });
            return null;
        };
        return proposed.index;
    }

    /// A workflow on another shard asking this one to start a run it will
    /// wait on: `[run_len:u16][run][ns_len:u16][ns][action_len:u16][action]`
    /// `[caller_len:u16][caller][workflow_len:u16][workflow][input]`.
    pub fn startRunFromInbox(_: *ActionsHandler, shard: *Shard, bytes: []const u8) void {
        var r: Fields = .{ .bytes = bytes };
        const run_id = r.next() orelse return;
        const ns = r.next() orelse return;
        const action = r.next() orelse return;
        const caller = r.next() orelse return;
        const workflow = r.next() orelse return;
        const input = bytes[r.off..];
        if (proposeInvoke(shard, ns, action, run_id, input, if (caller.len == 0) null else caller, if (workflow.len == 0) null else workflow) == null)
            log.err("shard {d}: action run {s} asked for by a workflow on another shard could not be started; that workflow run stays parked", .{ shard.id, run_id });
    }

    const Fields = struct {
        bytes: []const u8,
        off: usize = 0,

        fn next(self: *Fields) ?[]const u8 {
            if (self.off + 2 > self.bytes.len) return null;
            const n: usize = std.mem.readInt(u16, self.bytes[self.off..][0..2], .little);
            if (self.off + 2 + n > self.bytes.len) return null;
            defer self.off += 2 + n;
            return self.bytes[self.off + 2 ..][0..n];
        }
    };

    /// The inbox payload `startRunFromInbox` reads, allocated with
    /// `allocator`; the receiving shard frees it.
    pub fn encodeStartRunMessage(allocator: Allocator, namespace: []const u8, run_id: []const u8, action_name: []const u8, input: []const u8, caller_run_id: ?[]const u8, caller_workflow_name: ?[]const u8) ?[]u8 {
        const parts = [_][]const u8{ run_id, homeOf(namespace), action_name, caller_run_id orelse "", caller_workflow_name orelse "" };
        var size: usize = input.len;
        for (parts) |p| {
            if (p.len > std.math.maxInt(u16)) return null;
            size += 2 + p.len;
        }
        const out = allocator.alloc(u8, size) catch return null;
        var off: usize = 0;
        for (parts) |p| {
            std.mem.writeInt(u16, out[off..][0..2], @intCast(p.len), .little);
            @memcpy(out[off + 2 ..][0..p.len], p);
            off += 2 + p.len;
        }
        @memcpy(out[off..], input);
        return out;
    }

    // ── The expire duty, and leadership ─────────────────────────────────

    /// The leader's tick: end leases past their deadline, as one expire of
    /// the earliest keys past due. A key whose lease moved on is dropped by
    /// the same entry. A new leader ends a lease only past a grace: the
    /// lease's deadline, and at least min(lease, 30 s) after it first ran
    /// in its term. Returns how many keys it named.
    pub fn expireDue(self: *ActionsHandler, shard: *Shard) u64 {
        self.resend(shard);
        if (!shard.raft_node.leaderReady()) return 0;
        const now = wallNs();
        if (self.duty_term != shard.raft_node.current_term) {
            self.duty_term = shard.raft_node.current_term;
            self.duty_since_ns = now;
            // Runs ready before this term had no leader to wake for them.
            shard.wake_workers = true;
            ActionsHandler.wakeWorkers(shard);
            shard.wake_workers = false;
        }
        if (self.expiring != null) return 0;
        var items: std.ArrayListUnmanaged(u8) = .empty;
        defer items.deinit(self.allocator);
        var n: u32 = 0;
        var it = readOf(shard).range(machine.Deadlines, "");
        while (it.next() catch null) |item| {
            if (n == ae.MAX_EXPIRE_ITEMS) break;
            const k = machine.parseDeadlineKey(item.key[1..]) orelse continue;
            if (k.expiry_ns > now) break;
            if (getRun(shard, k.ns, k.run)) |run| if (run.status == .leased and run.expiry_ns == k.expiry_ns) {
                const grace = @as(u64, @min(run.lease_ms, MAX_GRACE_MS)) *| std.time.ns_per_ms;
                if (now < self.duty_since_ns +| grace) continue;
            };
            var ib: [ae.ExpireItem.max_size]u8 = undefined;
            const bytes = ae.ExpireItem.encodeInto(&ib, .{ .ns = k.ns, .action = item.value.v.action, .run = k.run, .expiry_ns = k.expiry_ns }) catch continue;
            items.appendSlice(self.allocator, bytes) catch break;
            n += 1;
        }
        if (n == 0) return 0;
        const proposed = propose(shard, .action_expire, ae.Expire.Value{ .items = .{ .bytes = items.items, .count = n } }) catch |err| {
            if (!shard.quietHalted(err)) log.warn("shard {d}: leases past their deadline not ended: {s}", .{ shard.id, @errorName(err) });
            return 0;
        };
        self.expiring = proposed.index;
        return n;
    }

    fn resend(self: *ActionsHandler, shard: *Shard) void {
        if (self.unsent.items.len == 0) return;
        const all = self.unsent.toOwnedSlice(self.allocator) catch return;
        defer self.allocator.free(all);
        for (all) |u| self.send(shard, u.target, u.tag, u.bytes);
    }

    /// This node stopped leading: what it proposed may never apply, and the
    /// next leader decides afresh. Awaits waiting on a deliver are told.
    pub fn stoppedLeading(self: *ActionsHandler, shard: *Shard) void {
        for (self.pending.items) |pd| {
            shard.deliverDeferredRefusal(pd.reply_to, pd.request_id, .unavailable, .{ .reason = .leader_unknown, .ran = .unknown, .message = "unavailable: this node stopped leading before the task was given; it may have been — retry with the same request key" });
            if (pd.remember) |r| self.allocator.free(r);
        }
        self.pending.clearRetainingCapacity();
        self.forgetLeading();
    }

    fn forgetLeading(self: *ActionsHandler) void {
        var rit = self.reserved.keyIterator();
        while (rit.next()) |k| self.allocator.free(k.*);
        self.reserved.clearRetainingCapacity();
        var eit = self.extending.keyIterator();
        while (eit.next()) |k| self.allocator.free(k.*);
        self.extending.clearRetainingCapacity();
        var mit = self.remembered.iterator();
        while (mit.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.ns);
            self.allocator.free(e.value_ptr.run);
        }
        self.remembered.clearRetainingCapacity();
        self.expiring = null;
        self.duty_term = 0;
    }

    pub fn actionCount(shard: *Shard) usize {
        return @intCast(storeOf(shard).family(machine.Defs.ID).count);
    }

    pub fn runCount(shard: *Shard) usize {
        return @intCast(storeOf(shard).family(machine.Runs.ID).count);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn parseInvokeValue(allocator: Allocator, value: []const u8) error{ Empty, BadFlag, Truncated, LabelsNotObject, OutOfMemory }!struct { labels: ?[]const u8, input: []const u8 } {
    if (value.len == 0) return error.Empty;
    switch (value[0]) {
        0 => return .{ .labels = null, .input = value[1..] },
        1 => {},
        else => return error.BadFlag,
    }
    if (value.len < 3) return error.Truncated;
    const labels_len: usize = std.mem.readInt(u16, value[1..3], .little);
    if (3 + labels_len > value.len) return error.Truncated;
    const labels = value[3 .. 3 + labels_len];
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, labels, .{}) catch |err| return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.LabelsNotObject,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return error.LabelsNotObject;
    return .{ .labels = labels, .input = value[3 + labels_len ..] };
}

/// Whether the worker's labels hold every label the run requires, each with
/// the same value. Both are JSON objects, e.g. {"gpu":true,"region":"us-east"}.
fn labelsMatch(allocator: Allocator, required_json: []const u8, worker_json: []const u8) bool {
    const req_parsed = std.json.parseFromSlice(std.json.Value, allocator, required_json, .{}) catch return false;
    defer req_parsed.deinit();
    const wrk_parsed = std.json.parseFromSlice(std.json.Value, allocator, worker_json, .{}) catch return false;
    defer wrk_parsed.deinit();
    const req_obj = switch (req_parsed.value) {
        .object => |o| o,
        else => return false,
    };
    const wrk_obj = switch (wrk_parsed.value) {
        .object => |o| o,
        else => return false,
    };
    for (req_obj.keys(), req_obj.values()) |rkey, rval| {
        const wval = wrk_obj.get(rkey) orelse return false;
        if (!jsonValEq(rval, wval)) return false;
    }
    return true;
}

fn jsonValEq(a: std.json.Value, b: std.json.Value) bool {
    return switch (a) {
        .null => b == .null,
        .bool => |va| b == .bool and b.bool == va,
        .integer => |va| b == .integer and b.integer == va,
        .float => |va| b == .float and b.float == va,
        .string => |va| b == .string and std.mem.eql(u8, va, b.string),
        .number_string => |va| b == .number_string and std.mem.eql(u8, va, b.number_string),
        .array, .object => false,
    };
}

fn sendActionResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .ok, .action_deleted => shard.sendOkResponse(conn, request_id, ""),
        .err => |e| shard.sendRefusal(conn, request_id, e.code.toStatus(), e.refusal()),
        .action_registered => |r| shard.sendOkResponse(conn, request_id, r.name),
        .action_invoked => |i| {
            // [run_id_len:u16][run_id][has_output:u8]
            var buf: [2 + 64 + 1]u8 = undefined;
            if (i.run_id.len > 64) return shard.sendErrorResponse(conn, request_id, .internal_error, "internal error: run id too long");
            std.mem.writeInt(u16, buf[0..2], @intCast(i.run_id.len), .little);
            @memcpy(buf[2..][0..i.run_id.len], i.run_id);
            buf[2 + i.run_id.len] = 0;
            shard.sendOkResponse(conn, request_id, buf[0 .. 3 + i.run_id.len]);
        },
        .action_list_result => |l| shard.sendOkResponse(conn, request_id, l.data),
        else => shard.sendErrorResponse(conn, request_id, .internal_error, "unhandled action response"),
    }
}
