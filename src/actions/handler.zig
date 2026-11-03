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
const InfoResponse = table_run.op("action.info").response;
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

/// Propose a schema'd action entry, encoded in the shard's entry buffer
/// when it fits (a run's input can make an invoke larger).
fn propose(shard: *Shard, comptime etype: EntryType, body: anytype) !persistence.ProposeResult {
    const P = ae.Payload(@backingInt(etype));
    const size = try P.encodedSize(body);
    if (size <= shard.entry_buf.len) return shard.raft_node.propose(etype, Flags.NONE, try P.encodeInto(shard.entry_buf[0..size], body));
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
    /// The expire proposed and not yet applied: one at a time.
    expiring: ?u64 = null,
    /// The term the duty last ran in, and when it first ran in it.
    duty_term: u64 = 0,
    duty_since_ns: u64 = 0,
    /// When runs released with a delay are due, the earliest `MAX_DUE` of
    /// them, as a max-heap: the duty wakes awaits as each comes. Leader
    /// memory; a new leader reads them from the store again.
    due: std.ArrayListUnmanaged(u64) = .empty,
    /// Reading due times from the store, `REBUILD_CHUNK` keys a tick: the
    /// key it stopped after, or none at the start, and the earliest time
    /// it looks for (those before it are held).
    rebuilding: bool = false,
    rebuild_after: std.ArrayListUnmanaged(u8) = .empty,
    rebuild_from: u64 = 0,
    /// Counts wakes for runs ready here, and wakes from other shards: an
    /// await asking other shards when one it could use came asks again
    /// rather than wait out its block time.
    wake_local: u64 = 0,
    wake_peer: u64 = 0,
    /// The earliest due time dropped while `due` was full, if any: every
    /// time held is before it, and any at or after it is dropped too.
    dropped: ?u64 = null,
    /// Awaits woken this pass, to run again once the pass is over.
    reruns: std.ArrayListUnmanaged(Rerun) = .empty,
    /// How runs a workflow here waits on ended, sent by their own shards;
    /// dropped if no workflow takes them in `FINISHED_KEEP_MS`.
    finished: std.StringHashMapUnmanaged(Finished) = .empty,
    /// When this shard last asked about a run on another shard, by run id.
    asked: std.StringHashMapUnmanaged(u64) = .empty,
    /// Runs another shard's workflow asked this one to start, not yet
    /// applied (or refused for want of their action), by when: asked
    /// about meanwhile, they aren't lost. Kept `FINISHED_KEEP_MS`.
    starting: std.StringHashMapUnmanaged(u64) = .empty,
    /// Messages for other shards the inbox had no room for; sent again each
    /// duty tick.
    unsent: std.ArrayListUnmanaged(Unsent) = .empty,
    /// action.status copies a run's output and error here: the answer is
    /// encoded after the store may have moved.
    status_buf: ?[]u8 = null,
    /// action.info copies the action's owner here: the answer is encoded
    /// after the handler returns, so its own stack is gone.
    info_owner_buf: [action_ops.MAX_OWNER]u8 = undefined,

    const Pending = struct {
        index: u64,
        term: u64,
        reply_to: ReplyTo,
        request_id: u64,
    };

    const Rerun = struct { reply_to: ReplyTo, request_id: u64, bytes: []u8 };

    pub const Finished = struct { status: ActionRunStatus, outcome: []u8, output: []u8, at_ms: u64 };

    const Unsent = struct { target: u16, tag: @import("../node/inbox.zig").Tag, bytes: []u8 };

    /// How long a run's result from another shard waits for its workflow
    /// (and a run another shard asked for waits to start, and an ask is
    /// remembered), and how long a workflow waits before asking again.
    const FINISHED_KEEP_MS: u64 = 10 * 60 * 1000;
    const ASK_EVERY_MS: u64 = 5000;
    /// Delayed retries the duty keeps a due time for; past it, every tick
    /// wakes.
    pub const MAX_DUE: usize = 4096;
    /// Ready keys one duty tick reads while rebuilding `due`.
    pub const REBUILD_CHUNK: usize = 1024;

    /// Requests and entries as clients and the log make them, for tests.
    pub const for_tests = test_support;

    pub fn init(allocator: Allocator) ActionsHandler {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ActionsHandler) void {
        self.forgetLeading();
        self.reserved.deinit(self.allocator);
        self.pending.deinit(self.allocator);
        self.extending.deinit(self.allocator);
        self.due.deinit(self.allocator);
        self.rebuild_after.deinit(self.allocator);
        for (self.reruns.items) |r| self.allocator.free(r.bytes);
        self.reruns.deinit(self.allocator);
        var it = self.finished.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.outcome);
            self.allocator.free(e.value_ptr.output);
        }
        self.finished.deinit(self.allocator);
        freeKeys(self.allocator, &self.asked);
        self.asked.deinit(self.allocator);
        freeKeys(self.allocator, &self.starting);
        self.starting.deinit(self.allocator);
        for (self.unsent.items) |u| self.allocator.free(u.bytes);
        self.unsent.deinit(self.allocator);
        if (self.status_buf) |b| self.allocator.free(b);
    }

    fn freeKeys(allocator: Allocator, map: anytype) void {
        var kit = map.keyIterator();
        while (kit.next()) |k| allocator.free(k.*);
        map.clearRetainingCapacity();
    }

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.action_register, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.action_invoke, dispatchAction, preRouteByAction);
        // The run id's bits name the partition the run lives on.
        dispatcher.registerWithRoute(.action_status, table_run.handler("action.status", ActionsHandler, runStatus), run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.action_info, table_run.handler("action.info", ActionsHandler, runInfo), preRouteByAction);
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
        const proposed = propose(shard, .action_invoke, ae.Invoke.Value{ .ns = ns, .action = name, .run = run_id, .input = parsed.input, .labels = parsed.labels, .source = .direct, .caller_run = null, .caller_workflow = null, .caller_shard = null }) catch |err|
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

    /// `[count:u32](entry)*[has_more:u8][cursor_len:u16]`, each entry as
    /// `writeListEntry` writes it: the actions in the namespace whose names
    /// start with the key.
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
        while (try it.next()) |item| try writeEntry(item.key[1 + ns.len ..], item.value.v, w);
        try w.writeByte(0);
        try w.writeInt(u16, 0, .little);
    }

    /// One action as its list answers it: `[name_len:u16][name][value_len:u32]`
    /// then `[version:u32][owner_len:u16][owner][timeout_ms:u64][max_attempts:u32][created_at_ms:i64]`.
    /// Nothing for an action this shard doesn't hold.
    pub fn writeListEntry(shard: *Shard, namespace: []const u8, name: []const u8, w: *std.Io.Writer) std.Io.Writer.Error!void {
        const def = getDef(shard, homeOf(namespace), name) orelse return;
        try writeEntry(name, def, w);
    }

    fn writeEntry(name: []const u8, def: machine.Def.Value, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.writeInt(u16, @intCast(name.len), .little);
        try w.writeAll(name);
        try w.writeInt(u32, @intCast(4 + 2 + def.owner.len + 8 + 4 + 8), .little);
        try w.writeInt(u32, def.version, .little);
        try w.writeInt(u16, @intCast(def.owner.len), .little);
        try w.writeAll(def.owner);
        try w.writeInt(u64, def.lease_ms, .little);
        try w.writeInt(u32, def.max_attempts, .little);
        try w.writeInt(i64, nsToMs(def.created_ns), .little);
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
            const b = try self.allocator.alloc(u8, action_ops.MAX_OUTPUT + action_ops.MAX_ERROR + action_ops.MAX_WORKER_ID + action_ops.MAX_OUTCOME);
            self.status_buf = b;
            break :blk b;
        };
        var used: usize = 0;
        const output: ?[]const u8 = if (run.status == .completed) try copyInto(buf, &used, run.result, action_ops.MAX_OUTPUT) else null;
        const error_message: ?[]const u8 = if (run.@"error".len == 0) null else try copyInto(buf, &used, run.@"error", action_ops.MAX_ERROR);
        const worker_id: ?[]const u8 = if (run.holder.len == 0) null else try copyInto(buf, &used, run.holder, action_ops.MAX_WORKER_ID);
        const named_outcome: ?[]const u8 = if (run.status != .completed or run.outcome.len == 0) null else try copyInto(buf, &used, run.outcome, action_ops.MAX_OUTCOME);
        return .{
            .answer = .{
                .status = @fromBackingInt(@backingInt(statusOf(run))),
                .created_at_ms = nsToMs(run.created_ns),
                .started_at_ms = if (run.started_ns == 0) null else nsToMs(run.started_ns),
                .completed_at_ms = if (run.completed_ns == 0) null else nsToMs(run.completed_ns),
                // As a task counts it: the failures, and the attempt under way
                // or that completed it; an abandoned delivery is no attempt.
                .attempt = run.failures +| @intFromBool(run.status == .leased or run.status == .completed),
                .output = output,
                .error_message = error_message,
                .worker_id = worker_id,
                .outcome = named_outcome,
            },
        };
    }

    // ── Info ────────────────────────────────────────────────────────────

    /// An action's definition and its runs. Ready runs are its ready keys,
    /// up to READY_COUNTED. The other counts and the latency read every run
    /// in the namespace, so they are none past RUNS_SCANNED: runs are kept,
    /// and a namespace's grow without bound.
    fn runInfo(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(InfoResponse) {
        const ns = homeOf(req.namespace);
        const def = getDef(shard, ns, req.key) orelse
            return .{ .refused = .{ .status = .not_found, .refusal = .{ .reason = .not_found, .ran = .no, .message = machine.NO_ACTION } } };
        const owner_buf = &shard.actions_handler.info_owner_buf;
        const owner = owner_buf[0..@min(def.owner.len, owner_buf.len)];
        @memcpy(owner, def.owner[0..owner.len]);

        var ready: u64 = 0;
        var ready_capped = false;
        var pb: [KEY_BUF]u8 = undefined;
        var rb: [machine.MAX_KEY_LEN]u8 = undefined;
        if (storeKey(&pb, ns, machine.readyPrefix(&rb, req.key))) |prefix| {
            var it = readOf(shard).range(machine.Ready, prefix);
            while (it.next() catch null) |_| {
                if (ready == action_ops.READY_COUNTED) {
                    ready_capped = true;
                    break;
                }
                ready += 1;
            }
        }

        const durations = try shard.allocator.alloc(u64, action_ops.RUNS_SCANNED);
        defer shard.allocator.free(durations);
        const counts = countRuns(shard, ns, req.key, durations);
        const lat = if (counts.sampled) null else latencyOf(durations[0..counts.measured]);
        const counted = !counts.sampled;
        return .{ .answer = .{
            .owner = owner,
            .version = def.version,
            .timeout_ms = def.lease_ms,
            .max_attempts = def.max_attempts,
            .created_at_ms = nsToMs(def.created_ns),
            .ready = ready,
            .ready_capped = ready_capped,
            .too_many_runs = counts.sampled,
            .running = if (counted) counts.running else null,
            .completed = if (counted) counts.completed else null,
            .failed = if (counted) counts.failed else null,
            .timed_out = if (counted) counts.timed_out else null,
            .latency_count = if (counted) counts.measured else null,
            .latency_avg_ms = if (lat) |l| l.avg_ms else null,
            .latency_p50_ms = if (lat) |l| l.p50_ms else null,
            .latency_p95_ms = if (lat) |l| l.p95_ms else null,
            .latency_max_ms = if (lat) |l| l.max_ms else null,
        } };
    }

    const RunCounts = struct {
        scanned: u32 = 0,
        sampled: bool = false,
        running: u64 = 0,
        completed: u64 = 0,
        failed: u64 = 0,
        timed_out: u64 = 0,
        measured: usize = 0,
    };

    /// `action`'s runs by status among the first `durations.len` of the
    /// namespace's, and the completed ones' durations in ms.
    fn countRuns(shard: *Shard, ns: []const u8, action: []const u8, durations: []u64) RunCounts {
        var c: RunCounts = .{};
        var pb: [KEY_BUF]u8 = undefined;
        const p = storeKey(&pb, ns, "") orelse return c;
        var it = readOf(shard).range(machine.Runs, p);
        while (it.next() catch null) |item| {
            if (c.scanned == durations.len) {
                c.sampled = true;
                break;
            }
            c.scanned += 1;
            const run = item.value.v;
            if (!std.mem.eql(u8, run.action, action)) continue;
            switch (run.status) {
                .ready => {},
                .leased => c.running += 1,
                .failed => c.failed += 1,
                .timed_out => c.timed_out += 1,
                .completed => {
                    c.completed += 1;
                    // A run completed without a worker taking it has no
                    // duration to measure.
                    if (run.started_ns != 0 and run.completed_ns >= run.started_ns) {
                        durations[c.measured] = (run.completed_ns - run.started_ns) / std.time.ns_per_ms;
                        c.measured += 1;
                    }
                },
            }
        }
        return c;
    }

    const Latency = struct { avg_ms: u64, p50_ms: u64, p95_ms: u64, max_ms: u64 };

    /// The index of the `pct`th percentile of `n` sorted values: ceil(pct·n/100) − 1.
    fn nearestRank(n: usize, pct: usize) usize {
        return (pct * n + 99) / 100 - 1;
    }

    /// Completed runs' durations' mean, median, 95th percentile and longest.
    fn latencyOf(durations: []u64) ?Latency {
        if (durations.len == 0) return null;
        std.mem.sortUnstable(u64, durations, {}, std.sort.asc(u64));
        var total: u128 = 0;
        for (durations) |d| total += d;
        return .{
            .avg_ms = @intCast(total / durations.len),
            // Nearest rank: the smallest value at or above that share.
            .p50_ms = durations[nearestRank(durations.len, 50)],
            .p95_ms = durations[nearestRank(durations.len, 95)],
            .max_ms = durations[durations.len - 1],
        };
    }

    fn copyInto(buf: []u8, used: *usize, bytes: []const u8, max: usize) Error![]const u8 {
        if (bytes.len > max) return error.StoredTooLong;
        @memcpy(buf[used.*..][0..bytes.len], bytes);
        defer used.* += bytes.len;
        return buf[used.*..][0..bytes.len];
    }

    // ── Await: on the worker's shard ────────────────────────────────────

    /// What an await needs between its steps: whom it is for, the actions
    /// it names (the request's, then the others its worker registered),
    /// and its wait.
    const Want = struct {
        ns: []const u8,
        worker: []const u8,
        labels: ?[]const u8,
        request_key: ?[]const u8,
        block_ms: u64,
        names: Names,
        /// How many of `names` the request named: the rest are the
        /// worker's registered ones.
        asked: usize = 0,
    };

    /// Take a task of one of the worker's actions. With a request key, the
    /// delivery that key got is looked for first, wherever its run is;
    /// then an action this shard owns is delivered here, and one another
    /// shard owns is asked of that shard, one at a time. With nothing
    /// anywhere the await waits, and runs again when woken.
    fn runAwait(shard: *Shard, conn: *Connection, req: Request, actions: action_ops.ActionNames.Value, block_ms: u64, request_key: ?[]const u8) Error!table_run.Outcome(AwaitResponse) {
        const ns = homeOf(req.namespace);
        const worker_id = req.key;
        if (worker_id.len > 0) if (shard.worker_handler.refusesWork(ns, worker_id)) |no| return .{ .refused = .{ .status = .bad_request, .refusal = no } };
        var want: Want = .{ .ns = ns, .worker = worker_id, .labels = null, .request_key = request_key, .block_ms = block_ms, .names = .{} };
        if (worker_id.len > 0) if (shard.worker_handler.find(ns, worker_id)) |w| {
            want.labels = w.metadata_owned;
        };
        var it = actions.iterator();
        while (it.next() catch null) |name| want.names.add(name);
        if (want.names.len == 0) return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun("bad request: an await names at least one action") } };
        want.asked = want.names.len;
        // Refused now rather than once it has to wait, so an await is
        // refused the same whether or not work is ready.
        var key_buf: [waiter_pool_mod.MAX_WAITER_KEY]u8 = undefined;
        if (AwaitKey.build(&key_buf, want) == null) return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun("bad request: worker id, request key and action names too long to wait on") } };
        if (worker_id.len > 0) if (shard.worker_handler.find(ns, worker_id)) |w| for (w.processes.items) |p| {
            if (p.kind == .action) want.names.add(p.name_owned);
        };
        return awaitPass(shard, conn, req, want, request_key != null);
    }

    /// One pass over the await's actions: those here first, then those
    /// elsewhere, asked in turn. `again_only` looks only for the delivery
    /// the request key got; the pass after it takes new work.
    fn awaitPass(shard: *Shard, conn: *Connection, req: Request, want: Want, again_only: bool) Error!table_run.Outcome(AwaitResponse) {
        const self = shard.actions_handler;
        var remote: Names = .{};
        var any_local = false;
        for (want.names.items[0..want.names.len]) |name| {
            if (ownerOf(shard, want.ns, name) != shard.id) {
                remote.add(name);
                continue;
            }
            any_local = true;
            switch (try self.deliverHere(shard, conn.replyTo(), req.header.request_id, want.ns, name, want.worker, want.labels, want.request_key, again_only)) {
                .proposed => return .wait,
                .again => |task| return .{ .answer = .{ .task = task } },
                .none, .not_ready => {},
            }
        }
        if (remote.len > 0) {
            const a = Ask.create(shard.allocator, req, want, remote, again_only, any_local, self) catch return error.OutOfMemory;
            startAsk(shard, conn, req, a.nextName().?, a) catch |err| switch (err) {
                // No room to ask now: it waits, and a wake asks again.
                error.GatherBusy => return self.waitForTask(shard, conn, req, want),
                else => return error.OutOfMemory,
            };
            return .wait;
        }
        if (again_only) return awaitPass(shard, conn, req, want, false);
        return self.waitForTask(shard, conn, req, want);
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

    fn waitForTask(self: *ActionsHandler, shard: *Shard, conn: *Connection, req: Request, want: Want) Error!table_run.Outcome(AwaitResponse) {
        _ = self;
        if (want.block_ms == 0) return .{ .answer = .{ .task = null } };
        var key_buf: [waiter_pool_mod.MAX_WAITER_KEY]u8 = undefined;
        const key = AwaitKey.build(&key_buf, want) orelse
            return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun("bad request: worker id, request key and action names too long to wait on") } };
        if (!shard.waiter_pool.register(.{ .kind = .action_await, .reply_to = conn.replyTo(), .request_id = req.header.request_id, .key = key.bytes, .min_version = key.lengths, .timeout_ms = @intCast(@min(want.block_ms, waiter_pool_mod.MAX_BLOCK_MS)) })) {
            shard.refuseWaitersFull(conn, req.header.request_id);
        }
        return .wait;
    }

    /// A waiting await's key: `[ns][worker][request_key][names]`, the
    /// names as an await's list encodes them, the lengths and the names'
    /// count packed in the waiter's min_version.
    const AwaitKey = struct {
        bytes: []const u8,
        lengths: u64,

        fn build(buf: []u8, want: Want) ?AwaitKey {
            const rk = want.request_key orelse "";
            if (want.ns.len > 0xFFFF or want.worker.len > 0xFFFF or rk.len > 0xFFFF) return null;
            var w: schema.Writer = .{ .buf = buf };
            w.put(want.ns) catch return null;
            w.put(want.worker) catch return null;
            w.put(rk) catch return null;
            // The request's names must fit; the worker's registered ones go
            // in while they do.
            var n: usize = 0;
            for (want.names.items[0..@min(want.names.len, action_ops.MAX_AWAIT_ACTIONS)], 0..) |name, i| {
                const mark = w.pos;
                action_ops.ActionName.encode(&w, name) catch {
                    if (i < want.asked) return null;
                    w.pos = mark;
                    break;
                };
                n += 1;
            }
            return .{ .bytes = buf[0..w.pos], .lengths = (@as(u64, want.ns.len) << 48) | (@as(u64, want.worker.len) << 32) | (@as(u64, rk.len) << 16) | n };
        }

        const Parts = struct { ns: []const u8, worker: []const u8, request_key: []const u8, names: action_ops.ActionNames.Value };

        fn parse(key: []const u8, lengths: u64) ?Parts {
            const ns_len: usize = @intCast(lengths >> 48);
            const worker_len: usize = @intCast((lengths >> 32) & 0xFFFF);
            const rk_len: usize = @intCast((lengths >> 16) & 0xFFFF);
            const count: u32 = @intCast(lengths & 0xFFFF);
            if (ns_len + worker_len + rk_len > key.len) return null;
            const a = ns_len;
            const b = a + worker_len;
            const c = b + rk_len;
            return .{ .ns = key[0..a], .worker = key[a..b], .request_key = key[b..c], .names = .{ .bytes = key[c..], .count = count } };
        }
    };

    // ── Asking other shards ─────────────────────────────────────────────

    /// What an await asking other owners keeps between asks.
    const Ask = struct {
        req_bytes: []u8,
        ns: []u8,
        worker: []u8,
        labels: ?[]u8,
        request_key: ?[]u8,
        block_ms: u64,
        /// Every name the await tries, and those left to ask in this pass,
        /// each `[len:u16][name]`.
        names: []u8,
        left: []u8,
        left_off: usize = 0,
        again_only: bool,
        /// The wake counts when the pass began, and whether a wake for runs
        /// here concerns it (it names an action this shard owns).
        wake_local: u64,
        wake_peer: u64,
        any_local: bool,
        /// When its wait ends, on the monotonic clock: a pass begun again
        /// waits only what is left.
        deadline_ms: u64,
        asked: usize,

        fn create(allocator: Allocator, req: Request, want: Want, remote: Names, again_only: bool, any_local: bool, h: *const ActionsHandler) !*Ask {
            const a = try allocator.create(Ask);
            errdefer allocator.destroy(a);
            a.* = .{
                .req_bytes = try Shard.serializeRequest(allocator, req),
                .ns = try allocator.dupe(u8, want.ns),
                .worker = try allocator.dupe(u8, want.worker),
                .labels = if (want.labels) |l| try allocator.dupe(u8, l) else null,
                .request_key = if (want.request_key) |k| try allocator.dupe(u8, k) else null,
                .block_ms = want.block_ms,
                .names = try packNames(allocator, want.names),
                .left = try packNames(allocator, remote),
                .again_only = again_only,
                .wake_local = h.wake_local,
                .wake_peer = h.wake_peer,
                .any_local = any_local,
                .deadline_ms = @import("stdx").time.monotonicMs() +| want.block_ms,
                .asked = want.asked,
            };
            return a;
        }

        fn packNames(allocator: Allocator, names: Names) ![]u8 {
            var out: std.ArrayListUnmanaged(u8) = .empty;
            defer out.deinit(allocator);
            for (names.items[0..names.len]) |name| {
                var len: [2]u8 = undefined;
                std.mem.writeInt(u16, &len, @intCast(name.len), .little);
                try out.appendSlice(allocator, &len);
                try out.appendSlice(allocator, name);
            }
            return out.toOwnedSlice(allocator);
        }

        fn unpack(bytes: []const u8) Names {
            var names: Names = .{};
            var off: usize = 0;
            while (off + 2 <= bytes.len) {
                const n: usize = std.mem.readInt(u16, bytes[off..][0..2], .little);
                names.add(bytes[off + 2 ..][0..n]);
                off += 2 + n;
            }
            return names;
        }

        fn asWant(a: *const Ask) Want {
            return .{ .ns = a.ns, .worker = a.worker, .labels = a.labels, .request_key = a.request_key, .block_ms = a.deadline_ms -| @import("stdx").time.monotonicMs(), .names = unpack(a.names), .asked = a.asked };
        }

        fn free(state: *anyopaque, allocator: Allocator) void {
            const a: *Ask = @ptrCast(@alignCast(state));
            allocator.free(a.req_bytes);
            allocator.free(a.ns);
            allocator.free(a.worker);
            if (a.labels) |l| allocator.free(l);
            if (a.request_key) |k| allocator.free(k);
            allocator.free(a.names);
            allocator.free(a.left);
            allocator.destroy(a);
        }

        fn nextName(a: *Ask) ?[]const u8 {
            if (a.left_off + 2 > a.left.len) return null;
            const n: usize = std.mem.readInt(u16, a.left[a.left_off..][0..2], .little);
            defer a.left_off += 2 + n;
            return a.left[a.left_off + 2 ..][0..n];
        }

        const op: gather_mod.Op = .{ .merge = merge, .deinit = free, .orphaned = orphaned };

        /// The owner answered: its task is the await's. None asks the next
        /// owner; once every owner said none, the next pass begins, or the
        /// await waits. A wake while it asked starts the pass again.
        fn merge(shard: *Shard, conn: *Connection, request_id: u64, state: *anyopaque, bodies: []const []const u8) anyerror!void {
            const a: *Ask = @ptrCast(@alignCast(state));
            var diag: schema.Diagnostic = .{};
            const got = try DeliverResponse.decodeAll(bodies[0], &diag);
            if (got.task) |task| return table_run.answer(AwaitResponse, shard, conn, request_id, .{ .task = task });
            const req = try Request.parse(a.req_bytes);
            if (a.nextName()) |name| {
                // The state moves to the next ask, which frees it.
                return startAsk(shard, conn, req, name, try a.clone(shard.allocator));
            }
            const self = shard.actions_handler;
            // Only a wake it could use: from another shard, or for runs
            // here when it names an action this shard owns.
            const woken = self.wake_peer != a.wake_peer or (a.any_local and self.wake_local != a.wake_local);
            // Its wait is over: answered with no task, never asked again.
            if (!a.again_only and a.deadline_ms <= @import("stdx").time.monotonicMs()) return table_run.answer(AwaitResponse, shard, conn, request_id, .{ .task = null });
            const outcome = if (a.again_only or woken)
                try awaitPass(shard, conn, req, a.asWant(), false)
            else
                try self.waitForTask(shard, conn, req, a.asWant());
            switch (outcome) {
                .answer => |v| table_run.answer(AwaitResponse, shard, conn, request_id, v),
                .wait => conn.response_deferred = true,
                .refused => |r| shard.sendRefusal(conn, request_id, r.status, r.refusal),
                .park => unreachable,
            }
        }

        /// The await went away while its owner delivered: the task goes
        /// back to the run's shard, released as abandoned.
        fn orphaned(shard: *Shard, state: *anyopaque, bodies: []const []const u8) void {
            const a: *Ask = @ptrCast(@alignCast(state));
            var diag: schema.Diagnostic = .{};
            const got = DeliverResponse.decodeAll(bodies[0], &diag) catch return;
            const task = got.task orelse return;
            shard.actions_handler.sendAbandon(shard, a.ns, a.worker, task);
        }

        fn clone(a: *const Ask, allocator: Allocator) !*Ask {
            const c = try allocator.create(Ask);
            errdefer allocator.destroy(c);
            c.* = .{
                .req_bytes = try allocator.dupe(u8, a.req_bytes),
                .ns = try allocator.dupe(u8, a.ns),
                .worker = try allocator.dupe(u8, a.worker),
                .labels = if (a.labels) |l| try allocator.dupe(u8, l) else null,
                .request_key = if (a.request_key) |k| try allocator.dupe(u8, k) else null,
                .block_ms = a.block_ms,
                .names = try allocator.dupe(u8, a.names),
                .left = try allocator.dupe(u8, a.left),
                .left_off = a.left_off,
                .again_only = a.again_only,
                .wake_local = a.wake_local,
                .wake_peer = a.wake_peer,
                .any_local = a.any_local,
                .deadline_ms = a.deadline_ms,
                .asked = a.asked,
            };
            return c;
        }
    };

    /// Ask the owner of `name` for a task of it. `a` is the gather's from
    /// here, freed however it ends.
    fn startAsk(shard: *Shard, conn: *Connection, req: Request, name: []const u8, a: *Ask) !void {
        const body_value: DeliverRequest.Value = .{ .holder = a.worker, .labels = a.labels, .request_key = a.request_key, .again_only = a.again_only };
        const body_size = DeliverRequest.encodedSize(body_value) catch {
            Ask.free(a, shard.allocator);
            return error.OutOfMemory;
        };
        const body = shard.allocator.alloc(u8, body_size) catch {
            Ask.free(a, shard.allocator);
            return error.OutOfMemory;
        };
        defer shard.allocator.free(body);
        _ = DeliverRequest.encodeInto(body, body_value) catch unreachable;
        const parts = [_]gather_mod.Part{.{ .owner = ownerOf(shard, a.ns, name), .op = @backingInt(OpCode.action_deliver), .key = name, .body = body }};
        try shard.startGather(conn, req, &parts, &Ask.op, a);
        conn.response_deferred = true;
    }

    // ── Deliver: on the action's shard ──────────────────────────────────

    /// Another shard's await asking for a task of this shard's action. A
    /// leader whose term's entries haven't applied refuses it, and the
    /// worker retries.
    fn runDeliver(shard: *Shard, conn: *Connection, req: Request, holder: []const u8, labels: ?[]const u8, request_key: ?[]const u8, again_only: bool) Error!table_run.Outcome(DeliverResponse) {
        return switch (try shard.actions_handler.deliverHere(shard, conn.replyTo(), req.header.request_id, homeOf(req.namespace), req.key, holder, labels, request_key, again_only)) {
            .proposed => .wait,
            .again => |task| .{ .answer = .{ .task = task } },
            .none => .{ .answer = .{ .task = null } },
            .not_ready => error.NotLeading,
        };
    }

    const Delivered = union(enum) {
        /// A deliver was proposed: its await is answered when it applies.
        proposed,
        /// The request key's delivery, still held: answered again as is.
        again: Task,
        none,
        /// Not a leader whose term's entries have applied.
        not_ready,
    };

    /// The next ready run of `action` the worker may take, delivered to it:
    /// earliest due first, skipping runs already being delivered and runs
    /// whose labels the worker lacks. Only a leader whose term's entries
    /// have applied chooses: before that, a run delivered in the last term
    /// may still read as ready here.
    fn deliverHere(self: *ActionsHandler, shard: *Shard, reply_to: ReplyTo, request_id: u64, ns: []const u8, action: []const u8, holder: []const u8, labels: ?[]const u8, request_key: ?[]const u8, again_only: bool) Error!Delivered {
        if (!shard.raft_node.leaderReady()) return .not_ready;
        if (request_key) |rk| if (again(shard, ns, action, holder, rk)) |task| return .{ .again = task };
        if (again_only) return .none;
        const def = getDef(shard, ns, action) orelse return .none;
        const now = wallNs();
        var pb: [KEY_BUF]u8 = undefined;
        var rb: [machine.MAX_KEY_LEN]u8 = undefined;
        const prefix = storeKey(&pb, ns, machine.readyPrefix(&rb, action)) orelse return .none;
        var it = readOf(shard).range(machine.Ready, prefix);
        var looked: usize = 0;
        while (it.next() catch null) |item| {
            const tail = item.key[prefix.len..];
            if (tail.len < 8) continue;
            const due = std.mem.readInt(u64, tail[0..8], .big);
            if (due > now) break;
            const run_id = tail[8..];
            var kb: [KEY_BUF]u8 = undefined;
            const run_key = storeKey(&kb, ns, run_id) orelse continue;
            // Runs being delivered don't count toward what one ask looks at.
            if (self.reserved.contains(run_key)) continue;
            if (looked == MAX_LOOKED_AT) break;
            looked += 1;
            const run = getRun(shard, ns, run_id) orelse continue;
            if (run.labels) |required| {
                const have = labels orelse continue;
                if (!labelsMatch(self.allocator, required, have)) continue;
            }
            // Room first: once proposed, nothing here may fail.
            try self.pending.ensureUnusedCapacity(self.allocator, 1);
            try self.reserved.ensureUnusedCapacity(self.allocator, 1);
            const owned_key = try self.allocator.dupe(u8, run_key);
            const proposed = propose(shard, .action_deliver, ae.Deliver.Value{ .ns = ns, .action = action, .run = run_id, .due_ns = due, .holder = holder, .request_key = request_key, .lease_ms = def.lease_ms }) catch |err| {
                self.allocator.free(owned_key);
                if (!shard.quietHalted(err)) log.warn("shard {d}: run {s} not delivered: {s}", .{ shard.id, run_id, @errorName(err) });
                return .none;
            };
            self.reserved.putAssumeCapacity(owned_key, {});
            self.pending.appendAssumeCapacity(.{ .index = proposed.index, .term = proposed.term, .reply_to = reply_to, .request_id = request_id });
            return .proposed;
        }
        return .none;
    }

    /// The delivery `holder`'s request key got, if the holder holds it
    /// still: from the log's own record, so any leader finds it.
    fn again(shard: *Shard, ns: []const u8, action: []const u8, holder: []const u8, request_key: []const u8) ?Task {
        var kb: [KEY_BUF + action_ops.MAX_WORKER_ID + action_ops.MAX_REQUEST_KEY]u8 = undefined;
        var rb: [action_ops.MAX_WORKER_ID + 1 + action_ops.MAX_REQUEST_KEY]u8 = undefined;
        if (holder.len + 1 + request_key.len > rb.len) return null;
        const k = storeKey(&kb, ns, machine.requestKey(&rb, holder, request_key)) orelse return null;
        const rec = (readOf(shard).get(machine.Requests, k) catch return null) orelse return null;
        const run = getRun(shard, ns, rec.v.run) orelse return null;
        if (!std.mem.eql(u8, run.action, action) or run.status != .leased or !std.mem.eql(u8, run.holder, holder)) return null;
        if (run.token.term != rec.v.term or run.token.index != rec.v.index) return null;
        return taskOf(ns, rec.v.run, run);
    }

    /// The task a delivered run is.
    fn taskOf(ns: []const u8, run_id: []const u8, run: Run) Task {
        return .{
            .run = run_id,
            .namespace = ns,
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
    /// for choosing again either way. An await whose client is gone has the
    /// task handed back.
    fn deliverApplied(self: *ActionsHandler, shard: *Shard, e: *const Entry, result: *const Result) void {
        var diag: schema.Diagnostic = .{};
        const p = ae.Deliver.decodeAll(e.payload, &diag) catch return;
        var kb: [KEY_BUF]u8 = undefined;
        if (storeKey(&kb, p.ns, p.run)) |k| if (self.reserved.fetchRemove(k)) |kv| self.allocator.free(kv.key);
        const at = for (self.pending.items, 0..) |pd, i| {
            if (pd.index == e.header.index and pd.term == e.header.term) break i;
        } else return;
        const pd = self.pending.swapRemove(at);
        const task: ?Task = if (result.refused() != null) null else if (getRun(shard, p.ns, p.run)) |run| taskOf(p.ns, p.run, run) else null;
        if (task) |t| if (gone(shard, pd.reply_to)) return self.abandon(shard, p.ns, p.holder, t);
        deliverAnswer(shard, pd.reply_to, pd.request_id, task);
    }

    /// Whether an answer for `reply_to` has nowhere to go: a client of this
    /// shard's that closed. Another shard's or node's client is that one's
    /// to tell.
    fn gone(shard: *Shard, reply_to: ReplyTo) bool {
        return switch (reply_to) {
            .socket => |s| s.shard == shard.id and shard.localConnection(reply_to) == null,
            .remote => false,
        };
    }

    /// Hand a task back that no await took: released as abandoned, which
    /// counts no failure.
    fn abandon(self: *ActionsHandler, shard: *Shard, ns: []const u8, holder: []const u8, t: Task) void {
        _ = self;
        const run = getRun(shard, ns, t.run) orelse return;
        _ = propose(shard, .action_release, ae.Release.Value{ .ns = ns, .action = t.action, .run = t.run, .holder = holder, .request_key = run.request_key, .deliveries = t.deliveries, .token = .{ .term = t.term, .index = t.index }, .expiry_ns = run.expiry_ns, .abandoned = true, .retry = true, .delay_ms = 0, .@"error" = "" }) catch |err| {
            if (!shard.quietHalted(err)) log.warn("shard {d}: run {s} not handed back: {s}; its lease runs out", .{ shard.id, t.run, @errorName(err) });
        };
    }

    /// `[ns_len:u16][ns][holder_len:u16][holder][action_len:u16][action][run_len:u16][run][deliveries:u32][term:u64][index:u64]`.
    fn sendAbandon(self: *ActionsHandler, shard: *Shard, ns: []const u8, holder: []const u8, t: Task) void {
        const parts = [_][]const u8{ ns, holder, t.action, t.run };
        var size: usize = 4 + 8 + 8;
        for (parts) |p| size += 2 + p.len;
        const bytes = self.allocator.alloc(u8, size) catch return;
        var off: usize = 0;
        for (parts) |p| {
            std.mem.writeInt(u16, bytes[off..][0..2], @intCast(p.len), .little);
            @memcpy(bytes[off + 2 ..][0..p.len], p);
            off += 2 + p.len;
        }
        std.mem.writeInt(u32, bytes[off..][0..4], t.deliveries, .little);
        std.mem.writeInt(u64, bytes[off + 4 ..][0..8], t.term, .little);
        std.mem.writeInt(u64, bytes[off + 12 ..][0..8], t.index, .little);
        const owner = ownerOf(shard, ns, t.action);
        if (owner == shard.id) {
            defer self.allocator.free(bytes);
            return self.abandonFromInbox(shard, bytes);
        }
        self.send(shard, owner, .action_abandon, bytes);
    }

    pub fn abandonFromInbox(self: *ActionsHandler, shard: *Shard, bytes: []const u8) void {
        var r: Fields = .{ .bytes = bytes };
        const ns = r.next() orelse return;
        const holder = r.next() orelse return;
        const action = r.next() orelse return;
        const run_id = r.next() orelse return;
        if (r.off + 20 != bytes.len) return;
        const run = getRun(shard, ns, run_id) orelse return;
        var t = taskOf(ns, run_id, run);
        t.action = action;
        t.deliveries = std.mem.readInt(u32, bytes[r.off..][0..4], .little);
        t.term = std.mem.readInt(u64, bytes[r.off + 4 ..][0..8], .little);
        t.index = std.mem.readInt(u64, bytes[r.off + 12 ..][0..8], .little);
        // A client's shard knows the task, not whom it was for: the run
        // says, and the token fences it.
        self.abandon(shard, ns, if (holder.len > 0) holder else run.holder, t);
    }

    /// An await's (or an ask's) answer that found no one to take it: its
    /// client went away, was told at its deadline, or its gather ended. A
    /// task in it goes back to its run's shard.
    pub fn orphanedAnswer(shard: *Shard, op: u16, bytes: []const u8) void {
        if (op != @backingInt(OpCode.action_await) and op != @backingInt(OpCode.action_deliver)) return;
        const resp = proto.Response.parse(bytes) catch return;
        if (resp.header.status != @backingInt(proto.StatusCode.ok)) return;
        var diag: schema.Diagnostic = .{};
        const got = AwaitResponse.decodeAll(resp.data, &diag) catch return;
        const task = got.task orelse return;
        shard.actions_handler.sendAbandon(shard, task.namespace, "", task);
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

    /// Wake this shard's awaits for a run ready here: each that may find
    /// one runs again, as it first ran, with the wait it has left.
    pub fn wakeLocal(shard: *Shard) void {
        shard.actions_handler.wake_local +%= 1;
        wakeAwaits(shard, resolveLocal);
    }

    /// Wake this shard's awaits for a run another shard says is ready:
    /// only those naming an action another shard owns.
    pub fn wakeFromPeer(shard: *Shard) void {
        shard.actions_handler.wake_peer +%= 1;
        wakeAwaits(shard, resolveRemote);
    }

    fn wakeAwaits(shard: *Shard, resolver: waiter_pool_mod.WaiterPool.ResolverFn) void {
        const self = shard.actions_handler;
        shard.waiter_pool.notifyAny(.action_await, resolver, @ptrCast(shard));
        if (self.reruns.items.len == 0) return;
        const reruns = self.reruns.toOwnedSlice(self.allocator) catch return;
        defer self.allocator.free(reruns);
        for (reruns) |r| {
            defer self.allocator.free(r.bytes);
            shard.runHeld(r.bytes, r.reply_to, r.request_id);
        }
    }

    fn resolveLocal(waiter: *Waiter, ctx: *anyopaque) bool {
        return resolveAwait(waiter, @ptrCast(@alignCast(ctx)), .local);
    }

    fn resolveRemote(waiter: *Waiter, ctx: *anyopaque) bool {
        return resolveAwait(waiter, @ptrCast(@alignCast(ctx)), .remote);
    }

    /// A waiting await that may find a task now: it leaves the pool and
    /// runs again after the wake. False leaves it waiting.
    fn resolveAwait(waiter: *Waiter, shard: *Shard, from: enum { local, remote }) bool {
        const self = shard.actions_handler;
        const parts = AwaitKey.parse(waiter.key(), waiter.min_version) orelse return false;
        if (parts.worker.len > 0) if (shard.worker_handler.refusesWork(parts.ns, parts.worker)) |no| {
            shard.deliverDeferredRefusal(waiter.reply_to, waiter.request_id, .bad_request, no);
            return true;
        };
        if (!mayFind(shard, parts, from == .remote)) return false;
        const left = waitLeft(waiter, @import("stdx").time.monotonicMs());
        if (left == 0) return false;
        const bytes = rerunBytes(shard.allocator, waiter.request_id, parts, left) catch return false;
        self.reruns.append(self.allocator, .{ .reply_to = waiter.reply_to, .request_id = waiter.request_id, .bytes = bytes }) catch {
            shard.allocator.free(bytes);
            return false;
        };
        return true;
    }

    /// Whether a woken await may find a task: one of its actions has a
    /// ready run due here, or, for a wake from another shard, is that
    /// shard's.
    fn mayFind(shard: *Shard, parts: AwaitKey.Parts, remote: bool) bool {
        var it = parts.names.iterator();
        while (it.next() catch null) |name| if (mayFindOf(shard, parts.ns, name, remote)) return true;
        if (parts.worker.len > 0) if (shard.worker_handler.find(parts.ns, parts.worker)) |w| for (w.processes.items) |p| {
            if (p.kind == .action and mayFindOf(shard, parts.ns, p.name_owned, remote)) return true;
        };
        return false;
    }

    fn mayFindOf(shard: *Shard, ns: []const u8, action: []const u8, remote: bool) bool {
        if (ownerOf(shard, ns, action) != shard.id) return remote;
        var pb: [KEY_BUF]u8 = undefined;
        var rb: [machine.MAX_KEY_LEN]u8 = undefined;
        const prefix = storeKey(&pb, ns, machine.readyPrefix(&rb, action)) orelse return false;
        var it = readOf(shard).range(machine.Ready, prefix);
        const first = (it.next() catch return false) orelse return false;
        const tail = first.key[prefix.len..];
        return tail.len >= 8 and std.mem.readInt(u64, tail[0..8], .big) <= wallNs();
    }

    /// What a waiting await has left of its wait, at `now_ms`.
    fn waitLeft(waiter: *const Waiter, now_ms: u64) u64 {
        return waiter.expires_at_ms -| now_ms;
    }

    /// The await a waiter was, as a request, waiting `left_ms` at most.
    fn rerunBytes(allocator: Allocator, request_id: u64, parts: AwaitKey.Parts, left_ms: u64) ![]u8 {
        const body: AwaitRequest.Value = .{ .actions = parts.names, .block_ms = @min(left_ms, action_ops.MAX_AWAIT_MS), .request_key = if (parts.request_key.len == 0) null else parts.request_key };
        const value = try allocator.alloc(u8, try AwaitRequest.encodedSize(body));
        defer allocator.free(value);
        _ = try AwaitRequest.encodeInto(value, body);
        const req: Request = .{ .header = proto.RequestHeader.init(.action_await, request_id), .namespace = parts.ns, .key = parts.worker, .value = value, .options = "" };
        return Shard.serializeRequest(allocator, req);
    }

    // ── Reports: complete, fail, touch ──────────────────────────────────

    /// The run a report names, if in scope (namespace and action). A resend
    /// of the report that ended the delivery, one for a delivery since moved
    /// on, and one from another worker are told apart by the machine.
    fn reported(shard: *Shard, req: Request, action: []const u8) Error!Run {
        if (!shard.raft_node.leaderReady()) return error.NotLeading;
        const run = getRun(shard, homeOf(req.namespace), req.key) orelse return error.NotFound;
        if (!std.mem.eql(u8, run.action, action)) return error.NotFound;
        return run;
    }

    fn runComplete(shard: *Shard, _: *Connection, req: Request, action: []const u8, worker: []const u8, deliveries: u32, term: u64, index: u64, outcome: []const u8, result: []const u8) Error!table_run.Outcome(EmptyResponse) {
        const run = try reported(shard, req, action);
        const proposed = propose(shard, .action_settle, ae.Settle.Value{ .ns = homeOf(req.namespace), .action = action, .run = req.key, .holder = worker, .request_key = run.request_key, .deliveries = deliveries, .token = .{ .term = term, .index = index }, .expiry_ns = run.expiry_ns, .outcome = outcome, .result = result }) catch |err|
            return .{ .refused = .{ .status = persistence.failureStatus(err), .refusal = persistence.failureRefusal(err, "complete not persisted") } };
        recordReport(shard, req.namespace, worker, action, false);
        return .{ .park = .{ .proposed = proposed, .answer = answerEmpty } };
    }

    fn runFail(shard: *Shard, _: *Connection, req: Request, action: []const u8, worker: []const u8, deliveries: u32, term: u64, index: u64, retry: bool, abandoned: bool, delay_ms: u64, err_text: []const u8) Error!table_run.Outcome(EmptyResponse) {
        const run = try reported(shard, req, action);
        const proposed = propose(shard, .action_release, ae.Release.Value{ .ns = homeOf(req.namespace), .action = action, .run = req.key, .holder = worker, .request_key = run.request_key, .deliveries = deliveries, .token = .{ .term = term, .index = index }, .expiry_ns = run.expiry_ns, .abandoned = abandoned, .retry = retry, .delay_ms = delay_ms, .@"error" = err_text }) catch |err|
            return .{ .refused = .{ .status = persistence.failureStatus(err), .refusal = persistence.failureRefusal(err, "fail not persisted") } };
        if (!abandoned) recordReport(shard, req.namespace, worker, action, true);
        return .{ .park = .{ .proposed = proposed, .answer = answerEmpty } };
    }

    /// Keep a delivery: answered with the lease as it stands while more
    /// than half of it is left, or while an extend for it is in flight;
    /// otherwise an extend is proposed and answered when it applies.
    fn runTouch(shard: *Shard, _: *Connection, req: Request, action: []const u8, worker: []const u8, deliveries: u32, term: u64, index: u64) Error!table_run.Outcome(TouchResponse) {
        const self = shard.actions_handler;
        const run = try reported(shard, req, action);
        const token: ae.Token.Value = .{ .term = term, .index = index };
        if (run.status != .leased or run.deliveries != deliveries or run.token.term != term or run.token.index != index or !std.mem.eql(u8, run.holder, worker))
            return .{ .refused = .{ .status = .conflict, .refusal = .{ .reason = .stale_report, .ran = .no, .message = machine.staleWhy(run, worker, deliveries, token) } } };
        var kb: [KEY_BUF]u8 = undefined;
        const run_key = storeKey(&kb, homeOf(req.namespace), req.key) orelse return error.NotFound;
        const half = (run.lease_ms *| std.time.ns_per_ms) / 2;
        if (run.expiry_ns -| wallNs() > half or self.extending.contains(run_key)) return .{ .answer = .{ .expires_at_ms = nsToMs(run.expiry_ns) } };
        try self.extending.ensureUnusedCapacity(self.allocator, 1);
        const owned = try self.allocator.dupe(u8, run_key);
        const proposed = propose(shard, .action_extend, ae.Extend.Value{ .ns = homeOf(req.namespace), .action = action, .run = req.key, .holder = worker, .token = token, .expiry_ns = run.expiry_ns, .held_since_ns = run.delivered_ns, .lease_ms = run.lease_ms }) catch |err| {
            self.allocator.free(owned);
            return .{ .refused = .{ .status = persistence.failureStatus(err), .refusal = persistence.failureRefusal(err, "touch not persisted") } };
        };
        self.extending.putAssumeCapacity(owned, {});
        return .{ .park = .{ .proposed = proposed, .answer = answerTouch } };
    }

    fn answerEmpty(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        table_run.answer(EmptyResponse, shard, conn, request_id, .{});
    }

    fn answerTouch(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        table_run.answer(TouchResponse, shard, conn, request_id, .{ .expires_at_ms = nsToMs(r.int(u64) orelse 0) });
    }

    /// A worker's report counts toward its stats where it is registered:
    /// here, or on its own shard, told through the inbox.
    fn recordReport(shard: *Shard, namespace: []const u8, worker: []const u8, action: []const u8, failed: bool) void {
        if (worker.len == 0) return;
        const ns = homeOf(namespace);
        const target = ownerOf(shard, ns, worker);
        if (target == shard.id) return countReport(shard, ns, worker, action, failed);
        const parts = [_][]const u8{ ns, worker, action };
        var size: usize = 1;
        for (parts) |p| size += 2 + p.len;
        const bytes = shard.allocator.alloc(u8, size) catch return;
        var off: usize = 0;
        for (parts) |p| {
            std.mem.writeInt(u16, bytes[off..][0..2], @intCast(p.len), .little);
            @memcpy(bytes[off + 2 ..][0..p.len], p);
            off += 2 + p.len;
        }
        bytes[off] = @intFromBool(failed);
        shard.actions_handler.send(shard, target, .worker_report, bytes);
    }

    fn countReport(shard: *Shard, ns: []const u8, worker: []const u8, action: []const u8, failed: bool) void {
        if (failed) shard.worker_handler.recordFailure(ns, worker, action) else shard.worker_handler.recordCompletion(ns, worker, action);
    }

    /// `[ns_len:u16][ns][worker_len:u16][worker][action_len:u16][action][failed:u8]`.
    pub fn reportFromInbox(shard: *Shard, bytes: []const u8) void {
        var r: Fields = .{ .bytes = bytes };
        const ns = r.next() orelse return;
        const worker = r.next() orelse return;
        const action = r.next() orelse return;
        if (r.off + 1 != bytes.len) return;
        countReport(shard, ns, worker, action, bytes[r.off] != 0);
    }

    // ── After an entry applies ──────────────────────────────────────────

    /// What the leader does once an action entry applies: answer the await
    /// its deliver was for, wake awaits for a run newly ready (or note when
    /// a delayed one is due), let its extend or expire be proposed again,
    /// and tell a workflow on another shard that a run it waits on
    /// finished. Every replica runs it; only a leader holds anything it
    /// acts on.
    pub fn afterApply(self: *ActionsHandler, shard: *Shard, e: *const Entry, result: *const Result) void {
        const etype: EntryType = @fromBackingInt(@intCast(e.header.entry_type));
        const leads = shard.raft_node.role == .leader;
        var diag: schema.Diagnostic = .{};
        switch (etype) {
            .action_deliver => if (leads) self.deliverApplied(shard, e, result),
            .action_invoke => {
                const p = ae.Invoke.decodeAll(e.payload, &diag) catch return;
                // Refused for want of the action, a run another shard's
                // workflow asked for stays starting, so that step waits for
                // the action as it always has (on the workflow's own shard
                // it fails at once); only a run whose invoke was lost is.
                const no_action = if (result.refused()) |r| r.refusal.reason == .not_found else false;
                if (!no_action) if (self.starting.fetchRemove(p.run)) |kv| self.allocator.free(kv.key);
                if (leads and result.refused() == null) shard.wake_workers = true;
            },
            .action_extend => {
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
            .action_settle => if (leads and result.refused() == null) {
                const p = ae.Settle.decodeAll(e.payload, &diag) catch return;
                self.tellWorkflow(shard, p.ns, p.run);
            },
            .action_release => if (leads and result.refused() == null) {
                const p = ae.Release.decodeAll(e.payload, &diag) catch return;
                if (p.delay_ms == 0) shard.wake_workers = true else self.noteDue(e.header.timestamp_ns +| p.delay_ms *| std.time.ns_per_ms);
                self.tellWorkflow(shard, p.ns, p.run);
            },
            else => {},
        }
    }

    /// Keep `due_ns` if it is among the earliest `MAX_DUE`.
    pub fn noteDue(self: *ActionsHandler, due_ns: u64) void {
        if (self.dropped) |first| if (due_ns >= first) return;
        const d = &self.due;
        if (d.items.len < MAX_DUE) {
            d.append(self.allocator, due_ns) catch return self.drop(due_ns);
            dueSiftUp(d.items, d.items.len - 1);
            return;
        }
        if (due_ns >= d.items[0]) return self.drop(due_ns);
        self.drop(d.items[0]);
        d.items[0] = due_ns;
        dueSiftDown(d.items, 0);
    }

    fn drop(self: *ActionsHandler, due_ns: u64) void {
        self.dropped = @min(self.dropped orelse due_ns, due_ns);
    }

    fn dueSiftUp(h: []u64, start: usize) void {
        var i = start;
        while (i > 0) {
            const parent = (i - 1) / 2;
            if (h[parent] >= h[i]) return;
            std.mem.swap(u64, &h[parent], &h[i]);
            i = parent;
        }
    }

    fn dueSiftDown(h: []u64, start: usize) void {
        var i = start;
        while (true) {
            var big = i;
            for ([_]usize{ 2 * i + 1, 2 * i + 2 }) |c| if (c < h.len and h[c] > h[big]) {
                big = c;
            };
            if (big == i) return;
            std.mem.swap(u64, &h[big], &h[i]);
            i = big;
        }
    }

    /// Start reading the due times dropped from the store again: all of
    /// them, the list cleared, if the earliest dropped is 0.
    pub fn startRebuild(self: *ActionsHandler) void {
        self.rebuild_from = self.dropped orelse return;
        if (self.rebuild_from == 0) self.due.clearRetainingCapacity();
        self.dropped = null;
        self.rebuilding = true;
        self.rebuild_after.clearRetainingCapacity();
    }

    /// Read the next `REBUILD_CHUNK` ready keys' due times, if rebuilding:
    /// a large backlog is read over several ticks, never in one. A time
    /// that passed while it read is woken for as it is found.
    pub fn stepRebuild(self: *ActionsHandler, shard: *Shard, now: u64) void {
        if (!self.rebuilding) return;
        var it = if (self.rebuild_after.items.len == 0) readOf(shard).range(machine.Ready, "") else readOf(shard).rangeAfter(machine.Ready, "", self.rebuild_after.items);
        var read: usize = 0;
        var passed = false;
        defer if (passed) ActionsHandler.wakeWorkers(shard);
        while (it.next() catch null) |item| {
            const k = item.key;
            const rest = k[1 + @as(usize, k[0]) ..];
            if (std.mem.indexOfScalar(u8, rest, 0)) |sep| if (rest.len >= sep + 9) {
                const due = std.mem.readInt(u64, rest[sep + 1 ..][0..8], .big);
                if (due >= self.rebuild_from) {
                    if (due > now) self.noteDue(due) else passed = true;
                }
            };
            read += 1;
            if (read == REBUILD_CHUNK) {
                self.rebuild_after.clearRetainingCapacity();
                self.rebuild_after.appendSlice(self.allocator, k) catch {
                    // Read it all again next tick.
                    self.rebuilding = false;
                    self.dropped = 0;
                };
                return;
            }
        }
        self.rebuilding = false;
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
        const target = run.caller_shard orelse shard.router.partitionToShard(run_id_mod.extractPartition(caller) orelse return);
        if (target == shard.id) return;
        self.sendFinished(shard, target, run_id, statusOf(run), if (run.status == .completed) run.outcome else "", if (run.status == .completed) run.result else "");
    }

    fn finishedStatus(s: machine.Status) bool {
        return switch (s) {
            .completed, .failed, .timed_out => true,
            .ready, .leased => false,
        };
    }

    /// `[run_len:u16][run][status:u8][outcome_len:u16][outcome][output]`.
    /// A run its shard has no record of, and isn't starting, goes as
    /// `cancelled`: its invoke was lost.
    fn sendFinished(self: *ActionsHandler, shard: *Shard, target: u16, run_id: []const u8, status: ActionRunStatus, outcome: []const u8, output: []const u8) void {
        const bytes = self.allocator.alloc(u8, 2 + run_id.len + 1 + 2 + outcome.len + output.len) catch return;
        std.mem.writeInt(u16, bytes[0..2], @intCast(run_id.len), .little);
        @memcpy(bytes[2..][0..run_id.len], run_id);
        bytes[2 + run_id.len] = @backingInt(status);
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
    /// on ended. Kept until the workflow takes it, or `FINISHED_KEEP_MS`.
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
        self.finished.put(self.allocator, k, .{ .status = status, .outcome = o, .output = out, .at_ms = @import("stdx").time.monotonicMs() }) catch {
            self.allocator.free(k);
            self.allocator.free(o);
            self.allocator.free(out);
        };
    }

    /// A workflow here that has heard nothing of a run asks its shard, at
    /// most once per `ASK_EVERY_MS`; the answer comes as `action_finished`.
    pub fn askResult(self: *ActionsHandler, shard: *Shard, target: u16, ns: []const u8, run_id: []const u8) void {
        const now = @import("stdx").time.monotonicMs();
        if (self.asked.getPtr(run_id)) |last| {
            if (now -| last.* < ASK_EVERY_MS) return;
            last.* = now;
        } else {
            const k = self.allocator.dupe(u8, run_id) catch return;
            self.asked.put(self.allocator, k, now) catch return self.allocator.free(k);
        }
        const bytes = self.allocator.alloc(u8, 2 + ns.len + run_id.len) catch return;
        std.mem.writeInt(u16, bytes[0..2], @intCast(ns.len), .little);
        @memcpy(bytes[2..][0..ns.len], ns);
        @memcpy(bytes[2 + ns.len ..], run_id);
        self.send(shard, target, .action_result_ask, bytes);
    }

    /// `[ns_len:u16][ns][run]`: answered if the run finished, or if this
    /// shard has no such run and isn't starting it, which means its invoke
    /// was lost.
    pub fn resultAskFromInbox(self: *ActionsHandler, shard: *Shard, from: u16, bytes: []const u8) void {
        if (bytes.len < 2) return;
        const ns_len: usize = std.mem.readInt(u16, bytes[0..2], .little);
        if (2 + ns_len > bytes.len) return;
        const ns = bytes[2..][0..ns_len];
        const run_id = bytes[2 + ns_len ..];
        // A leader whose term's entries haven't applied may yet apply the
        // invoke: it says nothing, and the workflow asks again.
        if (!shard.raft_node.leaderReady()) return;
        if (getRun(shard, ns, run_id)) |run| {
            if (finishedStatus(run.status)) self.sendFinished(shard, from, run_id, statusOf(run), if (run.status == .completed) run.outcome else "", if (run.status == .completed) run.result else "");
            return;
        }
        if (self.starting.contains(run_id)) return;
        self.sendFinished(shard, from, run_id, .cancelled, "", "");
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
        if (self.asked.fetchRemove(run_id)) |kv| self.allocator.free(kv.key);
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
        const caller_shard: ?u16 = if (caller_run_id != null) @intCast(shard.id) else null;
        const ns = homeOf(namespace);
        if (!actionExists(shard, ns, action_name)) return null;
        const run_id = shard.run_id_gen.next(.action, shard.router.keyToPartitionNs(namespace, action_name), id_buf);
        const index = proposeInvoke(shard, ns, action_name, run_id, input orelse "", caller_run_id, caller_workflow_name, caller_shard) orelse return null;
        return .{ .id = run_id, .index = index };
    }

    fn proposeInvoke(shard: *Shard, ns: []const u8, action: []const u8, run_id: []const u8, input: []const u8, caller_run: ?[]const u8, caller_workflow: ?[]const u8, caller_shard: ?u16) ?u64 {
        const proposed = propose(shard, .action_invoke, ae.Invoke.Value{ .ns = ns, .action = action, .run = run_id, .input = input, .labels = null, .source = if (caller_run != null) .workflow else .direct, .caller_run = caller_run, .caller_workflow = caller_workflow, .caller_shard = caller_shard }) catch |err| {
            if (!shard.quietHalted(err)) log.err("shard {d}: action run {s} not started: {s}", .{ shard.id, run_id, @errorName(err) });
            return null;
        };
        return proposed.index;
    }

    /// A workflow on another shard asking this one to start a run it will
    /// wait on: `[run_len:u16][run][ns_len:u16][ns][action_len:u16][action]`
    /// `[caller_len:u16][caller][workflow_len:u16][workflow][input]`, from
    /// the shard `from`, where the workflow's run is. A run
    /// that couldn't be started is lost: asked about, it says so.
    pub fn startRunFromInbox(self: *ActionsHandler, shard: *Shard, from: u16, bytes: []const u8) void {
        var r: Fields = .{ .bytes = bytes };
        const run_id = r.next() orelse return;
        const ns = r.next() orelse return;
        const action = r.next() orelse return;
        const caller = r.next() orelse return;
        const workflow = r.next() orelse return;
        const input = bytes[r.off..];
        if (proposeInvoke(shard, ns, action, run_id, input, if (caller.len == 0) null else caller, if (workflow.len == 0) null else workflow, if (caller.len == 0) null else from) == null) {
            log.err("shard {d}: action run {s} asked for by a workflow on another shard could not be started; that workflow's step fails", .{ shard.id, run_id });
            return;
        }
        const k = self.allocator.dupe(u8, run_id) catch return;
        self.starting.put(self.allocator, k, @import("stdx").time.monotonicMs()) catch self.allocator.free(k);
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
    /// the earliest keys past due; a key whose lease moved on is dropped by
    /// the same entry. A new leader ends a lease only past a grace: the
    /// lease's deadline, and at least min(lease, 30 s) after it first ran
    /// in its term. Also wakes awaits for delayed retries now due, resends
    /// what the inbox refused, and drops results no workflow took. Returns
    /// how many keys it named.
    pub fn expireDue(self: *ActionsHandler, shard: *Shard) u64 {
        self.resend(shard);
        self.pruneAt(@import("stdx").time.monotonicMs());
        if (!shard.raft_node.leaderReady()) return 0;
        const now = wallNs();
        if (self.duty_term != shard.raft_node.current_term) {
            self.duty_term = shard.raft_node.current_term;
            self.duty_since_ns = now;
            // Runs ready before this term had no leader to wake for them,
            // and those due later none to note when.
            self.dropped = 0;
            self.startRebuild();
            self.stepRebuild(shard, now);
            ActionsHandler.wakeWorkers(shard);
        } else self.wakeDue(shard, now);
        if (self.expiring != null) return 0;
        var items: std.ArrayListUnmanaged(u8) = .empty;
        defer items.deinit(self.allocator);
        var n: u32 = 0;
        var it = readOf(shard).range(machine.Deadlines, "");
        while (it.next() catch null) |item| {
            if (n == ae.MAX_EXPIRE_ITEMS) break;
            const k = machine.parseDeadlineKey(item.key[1..]) orelse continue;
            if (k.expiry_ns > now) break;
            var holder: []const u8 = "";
            var request_key: ?[]const u8 = null;
            if (getRun(shard, k.ns, k.run)) |run| if (run.status == .leased and run.expiry_ns == k.expiry_ns) {
                const grace = @as(u64, @min(run.lease_ms, MAX_GRACE_MS)) *| std.time.ns_per_ms;
                if (now < self.duty_since_ns +| grace) continue;
                holder = run.holder;
                request_key = run.request_key;
            };
            var ib: [ae.ExpireItem.max_size]u8 = undefined;
            const bytes = ae.ExpireItem.encodeInto(&ib, .{ .ns = k.ns, .action = item.value.v.action, .run = k.run, .expiry_ns = k.expiry_ns, .holder = holder, .request_key = request_key }) catch continue;
            if (items.items.len + bytes.len > ae.MAX_EXPIRE_BYTES) break;
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

    /// Wake awaits once a delayed retry is due. With some dropped, the
    /// store is read again for them once the list drains to half, or the
    /// earliest dropped comes due (0 when a read must start over).
    pub fn wakeDue(self: *ActionsHandler, shard: *Shard, now: u64) void {
        self.stepRebuild(shard, now);
        const d = &self.due;
        var woke = false;
        var i: usize = 0;
        while (i < d.items.len) {
            if (d.items[i] <= now) {
                _ = d.swapRemove(i);
                woke = true;
            } else i += 1;
        }
        if (woke) {
            // What's left, a heap again.
            var k = d.items.len / 2;
            while (k > 0) {
                k -= 1;
                dueSiftDown(d.items, k);
            }
            ActionsHandler.wakeWorkers(shard);
        }
        // A time not held may have come due: the earliest dropped, or one a
        // running rebuild has yet to read. Woken for each tick until read.
        if ((self.rebuilding and self.rebuild_from <= now) or (self.dropped orelse std.math.maxInt(u64)) <= now) ActionsHandler.wakeWorkers(shard);
        const first = self.dropped orelse return;
        if (!self.rebuilding and (first <= now or d.items.len < MAX_DUE / 2)) {
            self.startRebuild();
            self.stepRebuild(shard, now);
        }
    }

    /// Drop what is older than `FINISHED_KEEP_MS` at `now` (monotonic ms):
    /// results no workflow took, asks, and runs asked to start.
    pub fn pruneAt(self: *ActionsHandler, now: u64) void {
        pruneOld(self.allocator, &self.asked, now);
        pruneOld(self.allocator, &self.starting, now);
        if (self.finished.count() == 0) return;
        var it = self.finished.iterator();
        var old: std.ArrayListUnmanaged([]const u8) = .empty;
        defer old.deinit(self.allocator);
        while (it.next()) |e| if (now -| e.value_ptr.at_ms > FINISHED_KEEP_MS) old.append(self.allocator, e.key_ptr.*) catch break;
        for (old.items) |k| if (self.takeFinished(k)) |f| self.freeFinished(f);
    }

    /// Entries of a run-id → time map older than `FINISHED_KEEP_MS`.
    fn pruneOld(allocator: Allocator, map: *std.StringHashMapUnmanaged(u64), now: u64) void {
        if (map.count() == 0) return;
        var old: std.ArrayListUnmanaged([]const u8) = .empty;
        defer old.deinit(allocator);
        var it = map.iterator();
        while (it.next()) |e| if (now -| e.value_ptr.* > FINISHED_KEEP_MS) old.append(allocator, e.key_ptr.*) catch break;
        for (old.items) |k| if (map.fetchRemove(k)) |kv| allocator.free(kv.key);
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
        }
        self.pending.clearRetainingCapacity();
        self.forgetLeading();
    }

    fn forgetLeading(self: *ActionsHandler) void {
        freeKeys(self.allocator, &self.reserved);
        freeKeys(self.allocator, &self.extending);
        self.due.clearRetainingCapacity();
        self.dropped = null;
        self.rebuilding = false;
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

// ═══════════════════════════════════════════════════════════════════════════════
// For tests: requests and entries as clients and the log make them
// ═══════════════════════════════════════════════════════════════════════════════

const test_support = struct {
    /// An await's body naming `names`.
    pub fn awaitBody(buf: []u8, names: []const []const u8, block_ms: u64, request_key: ?[]const u8) ![]const u8 {
        var lb: [4096]u8 = undefined;
        var lw: schema.Writer = .{ .buf = &lb };
        for (names) |n| try action_ops.ActionName.encode(&lw, n);
        return AwaitRequest.encodeInto(buf, .{ .actions = .{ .bytes = lb[0..lw.pos], .count = @intCast(names.len) }, .block_ms = block_ms, .request_key = request_key });
    }

    /// The task an await's answer body carries, if any.
    pub fn task(body: []const u8) !?Task {
        var diag: schema.Diagnostic = .{};
        return (try AwaitResponse.decodeAll(body, &diag)).task;
    }

    /// A complete of `t`'s delivery.
    pub fn completeBody(buf: []u8, t: Task, worker: []const u8, outcome: []const u8, result: []const u8) ![]const u8 {
        return table_run.op("action.complete").request.encodeInto(buf, .{ .action = t.action, .worker = worker, .deliveries = t.deliveries, .term = t.term, .index = t.index, .outcome = outcome, .result = result });
    }

    /// A fail of `t`'s delivery.
    pub fn failBody(buf: []u8, t: Task, worker: []const u8, retry: bool, err_text: []const u8) ![]const u8 {
        return table_run.op("action.fail").request.encodeInto(buf, .{ .action = t.action, .worker = worker, .deliveries = t.deliveries, .term = t.term, .index = t.index, .retry = retry, .abandoned = false, .delay_ms = 0, .@"error" = err_text });
    }

    /// An action entry of type `etype`, as the log holds it.
    pub fn entry(comptime etype: EntryType, body: anytype, term: u64, index: u64, stamp_ns: u64, buf: []u8) !Entry {
        const P = ae.Payload(@backingInt(etype));
        return entry_mod.buildEntry(etype, Flags.NONE, term, index, stamp_ns, try P.encodeInto(buf, body));
    }

    /// A register value: the lease (0 is the default) and attempts.
    pub fn registerValue(buf: *[11]u8, timeout_ms: u32, max_retries: u32) []const u8 {
        buf[0] = 0;
        std.mem.writeInt(u32, buf[1..5], timeout_ms, .little);
        std.mem.writeInt(u32, buf[5..9], max_retries, .little);
        std.mem.writeInt(u16, buf[9..11], 0, .little);
        return buf;
    }

    /// The run `id` in `ns`, from the shard's store.
    pub fn run(shard: *Shard, ns: []const u8, id: []const u8) ?Run {
        return getRun(shard, ns, id);
    }

    /// Action `name`'s version in `ns`.
    pub fn version(shard: *Shard, ns: []const u8, name: []const u8) ?u32 {
        return if (getDef(shard, ns, name)) |d| d.version else null;
    }

    pub fn waitLeft(waiter: *const Waiter, now_ms: u64) u64 {
        return ActionsHandler.waitLeft(waiter, now_ms);
    }

    /// How many names a waiting await's key holds, of `asked` the request
    /// named and the rest the worker registered; null when the request's
    /// own don't fit.
    pub fn awaitKeyNames(worker: []const u8, names: []const []const u8, asked: usize) ?u64 {
        var want: ActionsHandler.Want = .{ .ns = "default", .worker = worker, .labels = null, .request_key = null, .block_ms = 1000, .names = .{}, .asked = asked };
        for (names) |n| want.names.add(n);
        var buf: [waiter_pool_mod.MAX_WAITER_KEY]u8 = undefined;
        const key = ActionsHandler.AwaitKey.build(&buf, want) orelse return null;
        return key.lengths & 0xFFFF;
    }

    /// The first run the shard holds, in any namespace.
    pub fn anyRun(shard: *Shard) ?Run {
        var it = readOf(shard).range(machine.Runs, "");
        const first = (it.next() catch return null) orelse return null;
        return first.value.v;
    }
};

test "action info's latency: percentiles of the completed runs, none without one" {
    try std.testing.expectEqual(@as(?ActionsHandler.Latency, null), ActionsHandler.latencyOf(&.{}));
    // Two runs: the 95th percentile is the slower one, not the faster.
    var two = [_]u64{ 59, 6433 };
    try std.testing.expectEqual(@as(u64, 6433), ActionsHandler.latencyOf(&two).?.p95_ms);
    try std.testing.expectEqual(@as(u64, 59), ActionsHandler.latencyOf(&two).?.p50_ms);
    var one = [_]u64{7};
    const l1 = ActionsHandler.latencyOf(&one).?;
    try std.testing.expectEqual(@as(u64, 7), l1.p95_ms);
    var many: [100]u64 = undefined;
    for (&many, 0..) |*d, i| d.* = 100 - i; // 100 down to 1
    const l = ActionsHandler.latencyOf(&many).?;
    try std.testing.expectEqual(@as(u64, 50), l.avg_ms);
    try std.testing.expectEqual(@as(u64, 50), l.p50_ms);
    try std.testing.expectEqual(@as(u64, 95), l.p95_ms);
    try std.testing.expectEqual(@as(u64, 100), l.max_ms);
}
