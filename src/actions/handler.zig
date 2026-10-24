//! Actions Handler — registers action opcodes with Dispatcher.
//!
//! Actions are a Layer 2 "Intelligent Layer" that compose Layer 1 primitives
//! (KV + Queue) for durable action execution. The handler manages action
//! registration, invocation, status tracking, listing, and deletion.
//!
//! ## Opcode Range
//!
//!   Commands:   0x60–0x64  (register, invoke, status, list, delete)
//!   Responses:  0x65–0x68
//!
//! ## Actions Architecture
//!
//! - Registered actions are stored as `_action:{name}` in KV (via ActionMeta).
//! - Invocations create a run record `_action_run:{run_id}` and enqueue a task.
//! - Status checks read the run record from KV.
//! - List scans the `_action:` prefix in KV.
//! - Delete removes the action.
//!
//! Durable through the log: a write proposes an entry and parks its client
//! until the entry applies; in-memory state is rebuilt from the same
//! entries on startup via the ReplayRegistry.

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const ActionMeta = @import("types.zig").ActionMeta;
const ActionType = @import("types.zig").ActionType;
const RunStatus = @import("types.zig").RunStatus;

const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const router = @import("../node/router.zig");
const run_id_mod = @import("../node/run_id.zig");
const table_run = @import("../node/table_run.zig");
const action_ops = @import("../protocol/action_ops.zig");
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const waiter_pool_mod = @import("../node/waiter_pool.zig");
const ReplyTo = @import("../node/reply_to.zig").ReplyTo;
const WorkerRecord = @import("../worker/handler.zig").WorkerRecord;
const WorkerHandler = @import("../worker/handler.zig").WorkerHandler;

const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const Result = persistence.Result;
const EntryType = entry_mod.EntryType;
const Entry = entry_mod.Entry;
const Flags = entry_mod.Flags;

const ns_keys = @import("../namespace/handler.zig");

const CommandResult = result_mod.CommandResult;
const ActionRunStatus = CommandResult.ActionRunStatus;

const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;

// ═══════════════════════════════════════════════════════════════════════════════
// ActionsHandler
// ═══════════════════════════════════════════════════════════════════════════════

/// An action's timeout when its register names none.
pub const DEFAULT_ACTION_TIMEOUT_MS: u32 = 30_000;

/// Longest lease one touch may ask for: a touch can't park a run forever.
pub const MAX_TOUCH_EXTEND_MS: u32 = 60 * 60 * 1000;

pub const ActionsHandler = struct {
    /// Public reference to resolveActionAwait for cross-shard inbox notifications.
    pub const resolveActionAwaitFn = resolveActionAwait;

    allocator: Allocator,

    /// In-memory action registry, keyed by `defKey`: "ns\x00name", or the
    /// bare name in "default", so one name in two namespaces is two actions.
    actions: std.StringHashMap(ActionRecord),

    /// In-memory run store. run_id → RunRecord.
    /// Protected by runs_mu for cross-shard access from workflow handler.
    runs: std.StringHashMap(RunRecord),

    /// Mutex protecting runs for cross-shard access.
    runs_mu: @import("stdx").Mutex = .{},
    /// action.status copies a run's output and error here while it holds
    /// runs_mu, since the answer is encoded after it lets go. Allocated on
    /// first use.
    status_buf: ?[]u8 = null,

    /// Without a shard (unit tests) a write applies here: its result, which
    /// the answer points into.
    local_result: Result = .{},
    local_ver_buf: [12]u8 = undefined,

    /// Monotonic counter used to produce unique run IDs when shard is null (tests).
    null_shard_seq: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    const MAX_ACTION_NAME_LEN: usize = 256;
    const MAX_ACTIONS: usize = 10_000;

    pub const ActionRecord = struct {
        /// The map key (`defKey`), owned.
        key_owned: []const u8,
        name_owned: []const u8,
        namespace_owned: []const u8,
        /// Owner/team string from `--owner` (always an owned slice, possibly empty).
        owner_owned: []const u8,
        action_type: ActionType,
        version: u32,
        enabled: bool,
        created_at_ns: u64,
        /// Execution timeout in ms from `--timeout` (persisted).
        timeout_ms: u32 = DEFAULT_ACTION_TIMEOUT_MS,
        max_retries: u32 = 3,
    };

    pub const RunRecord = struct {
        run_id_owned: []const u8,
        action_name_owned: []const u8,
        /// The namespace the run was invoked in ("default" for none); a client
        /// reaches the run only from there.
        namespace_owned: []const u8,
        input_owned: ?[]const u8,
        labels_owned: ?[]const u8 = null,
        result_owned: ?[]const u8 = null,
        outcome_owned: ?[]const u8 = null,
        worker_id_owned: ?[]const u8 = null,
        error_owned: ?[]const u8 = null,
        source: u8 = 0, // 0 = direct, 1 = workflow, 2 = trigger
        caller_run_id_owned: ?[]const u8 = null,
        caller_workflow_name_owned: ?[]const u8 = null,
        status: ActionRunStatus,
        created_at_ms: i64,
        started_at_ms: ?i64,
        completed_at_ms: ?i64,
        attempt: u32 = 0,
        max_retries: u32 = 3,
        /// When the running attempt's lease began (its claim, or its last
        /// touch), and the length the last touch asked for; the lease runs
        /// for the longer of that and the action's `timeout_ms`, read at
        /// each sweep. Leader memory, like the claim.
        lease_from_ms: i64 = 0,
        lease_ms: ?u32 = null,
        /// A timeout for this attempt is proposed and not yet applied, so
        /// the next sweep doesn't propose another. Leader memory: cleared
        /// when an update applies, at a claim, and when this node stops
        /// leading.
        expiry_proposed: bool = false,
    };

    pub fn init(allocator: Allocator) ActionsHandler {
        return .{
            .allocator = allocator,
            .actions = std.StringHashMap(ActionRecord).init(allocator),
            .runs = std.StringHashMap(RunRecord).init(allocator),
        };
    }

    /// The registry key of action `name` in `namespace` (empty is
    /// "default"): "ns\x00name", or the bare name in "default", as the
    /// register entry carries it. Null if too long to be one, or if the
    /// name holds a NUL.
    pub fn defKey(buf: *[ns_keys.MAX_QUALIFIED_KEY]u8, namespace: []const u8, name: []const u8) ?[]const u8 {
        // A name holding a NUL could spell another namespace's key.
        if (std.mem.indexOfScalar(u8, name, 0) != null) return null;
        return ns_keys.qualifyKey(buf, namespace, name) catch null;
    }

    /// Whether `run` was invoked in `namespace` (empty is "default").
    fn runIn(run: *const RunRecord, namespace: []const u8) bool {
        return std.mem.eql(u8, run.namespace_owned, homeOf(namespace));
    }

    fn homeOf(namespace: []const u8) []const u8 {
        return if (namespace.len == 0) "default" else namespace;
    }

    pub fn deinit(self: *ActionsHandler) void {
        if (self.status_buf) |b| self.allocator.free(b);
        // Free all owned action names
        var ait = self.actions.iterator();
        while (ait.next()) |entry| {
            self.allocator.free(entry.value_ptr.key_owned);
            self.allocator.free(entry.value_ptr.namespace_owned);
            self.allocator.free(entry.value_ptr.name_owned);
            self.allocator.free(entry.value_ptr.owner_owned);
        }
        self.actions.deinit();

        // Free all owned run data
        var rit = self.runs.iterator();
        while (rit.next()) |entry| {
            self.allocator.free(entry.value_ptr.run_id_owned);
            self.allocator.free(entry.value_ptr.action_name_owned);
            self.allocator.free(entry.value_ptr.namespace_owned);
            if (entry.value_ptr.input_owned) |inp| self.allocator.free(inp);
            if (entry.value_ptr.labels_owned) |lbl| self.allocator.free(lbl);
            if (entry.value_ptr.result_owned) |res| self.allocator.free(res);
            if (entry.value_ptr.outcome_owned) |out| self.allocator.free(out);
            if (entry.value_ptr.worker_id_owned) |wid| self.allocator.free(wid);
            if (entry.value_ptr.error_owned) |err_msg| self.allocator.free(err_msg);
            if (entry.value_ptr.caller_run_id_owned) |crid| self.allocator.free(crid);
            if (entry.value_ptr.caller_workflow_name_owned) |cwn| self.allocator.free(cwn);
        }
        self.runs.deinit();
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.action_register, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.action_invoke, dispatchAction, preRouteByAction);
        // The run id's bits name the partition the run lives on.
        dispatcher.registerWithRoute(.action_status, table_run.handler("action.status", ActionsHandler, runStatus), run_id_mod.preRouteByRunId);
        dispatcher.registerWalk(.action_list, dispatchAction, localScanActions);
        dispatcher.registerWithRoute(.action_delete, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.action_list_runs, dispatchAction, preRouteByAction);
        // An await runs where its worker is registered, so the worker's
        // record (labels, draining, actions) is read on its own shard; the
        // runs it claims live on their actions' shards.
        dispatcher.registerWithRoute(.action_await, dispatchActionAwait, WorkerHandler.preRouteByWorker);
        dispatcher.registerWithRoute(.action_complete, dispatchActionTaskCmd, preRouteByActionValue);
        dispatcher.registerWithRoute(.action_fail, dispatchActionTaskCmd, preRouteByActionValue);
        dispatcher.registerWithRoute(.action_touch, dispatchActionTaskCmd, preRouteByActionValue);
    }

    fn preRouteByAction(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    /// Route complete/fail/touch by the leading action_name in the value.
    /// Value format: [action_name_len:u16][action_name][rest...]
    fn preRouteByActionValue(req: Request) ?u64 {
        if (parseLeadingName(req.value)) |action_name| {
            return router.hashKeyWithNamespace(req.namespace, action_name);
        }
        return 0;
    }

    fn dispatchAction(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.actions_handler.handleCommand(shard, req);
        defer shard.actions_handler.freeResult(result);
        if (result == .parked) {
            const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
            if (op == .action_register or op == .action_invoke) shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
            return shard.park(conn, req, result.parked, switch (op) {
                .action_register => answerRegister,
                .action_invoke => answerInvoke,
                .action_delete => answerDelete,
                else => answerOk,
            });
        }
        sendActionResponse(shard, conn, req.header.request_id, result);
    }

    fn answerRegister(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var ver_buf: [12]u8 = undefined;
        sendActionResponse(shard, conn, request_id, registeredFrom(result, &ver_buf));
    }

    fn answerInvoke(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        sendActionResponse(shard, conn, request_id, invokedFrom(result));
    }

    fn answerDelete(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        sendActionResponse(shard, conn, request_id, .{ .action_deleted = {} });
    }

    fn answerOk(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        shard.sendOkResponse(conn, request_id, "");
    }

    /// A run exists for workers to take: wake this shard's action_await
    /// waiters and set the other shards' `action_invoked` flag, whose
    /// workers may take it. Run once after an apply pass in which an invoke
    /// applied on the leader, whoever proposed it.
    pub fn wakeWorkers(shard: *Shard) void {
        // Use notifyAny because waiter keys are compound (action_name + worker_id).
        shard.waiter_pool.notifyAny(.action_await, resolveActionAwait, @ptrCast(shard));
        if (shard.peer_mailboxes) |mailboxes| {
            for (mailboxes, 0..) |mb, i| {
                if (i == shard.id) continue;
                mb.wake.set(.action_invoked);
            }
        }
    }

    /// Why a task command was refused, with the status the worker is told.
    const TaskError = struct { status: proto.StatusCode, message: []const u8, reason: proto.Reason = .unclassified };

    /// What a task command did: refused, proposed (the worker parks on
    /// it), or applied here without a shard.
    /// `repeat`: the run already has the outcome reported, so the report
    /// is answered ok with nothing proposed or counted again.
    const TaskOutcome = union(enum) { refused: TaskError, parked: persistence.ProposeResult, applied, repeat };

    /// Dispatch for action_complete, action_fail, action_touch opcodes.
    fn dispatchActionTaskCmd(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        switch (op) {
            .action_complete, .action_fail => {
                const outcome = if (op == .action_complete) shard.actions_handler.handleActionComplete(shard, req) else shard.actions_handler.handleActionFail(shard, req);
                switch (outcome) {
                    // Refused before anything was proposed.
                    .refused => |e| shard.sendRefusal(conn, req.header.request_id, e.status, .{ .reason = e.reason, .ran = .no, .message = e.message }),
                    .parked => |proposed| {
                        recordTask(shard, req, op);
                        shard.park(conn, req, proposed, answerOk);
                    },
                    .applied => {
                        recordTask(shard, req, op);
                        shard.sendOkResponse(conn, req.header.request_id, "");
                    },
                    .repeat => shard.sendOkResponse(conn, req.header.request_id, ""),
                }
            },
            .action_touch => {
                if (shard.actions_handler.handleActionTouch(req)) |e| {
                    shard.sendErrorResponse(conn, req.header.request_id, e.status, e.message);
                } else {
                    shard.sendOkResponse(conn, req.header.request_id, "");
                }
            },
            else => {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "unknown action task opcode");
            },
        }
    }

    /// A worker's report, taken: its stats. Counted where the report is
    /// accepted, not once it applies: they are this node's view of the
    /// worker, and only when the worker is registered on this shard, since
    /// another shard's registry is that shard's thread's to change.
    fn recordTask(shard: *Shard, req: Request, op: OpCode) void {
        const action_name = parseLeadingName(req.value);
        if (resolveWorkerShard(shard, req.namespace, req.key) == shard) {
            if (op == .action_complete) shard.worker_handler.recordCompletion(req.namespace, req.key, action_name) else shard.worker_handler.recordFailure(req.namespace, req.key, action_name);
        }
    }

    /// Extract the leading [name_len:u16][name] from a value buffer.
    /// Used to get the action_name from action_complete/fail value wire format.
    fn parseLeadingName(value: []const u8) ?[]const u8 {
        if (value.len < 2) return null;
        const name_len: usize = std.mem.readInt(u16, value[0..2], .little);
        if (2 + name_len > value.len) return null;
        return value[2 .. 2 + name_len];
    }

    /// Blocking wait for a task to be dispatched.  Workers call this to receive work.
    fn dispatchActionAwait(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        // Wire format: key = worker_id, value = [count:u32][type_len:u16][type]*
        // Extract the first task type as the action name for matching.
        const action_name = extractFirstTaskType(req.value) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "action name is required");
            return;
        };

        const worker_id = req.key;

        // A draining or unhealthy worker takes no new task.
        if (worker_id.len > 0) if (shard.worker_handler.refusesWork(req.namespace, worker_id)) |no_work| {
            shard.sendRefusal(conn, req.header.request_id, .bad_request, no_work);
            return;
        };
        if (action_name.len > MAX_ACTION_NAME_LEN) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "action name too long");
            return;
        }

        // The key it would wait under, built first: an await that could
        // not wait is refused the same whether or not work is pending.
        const namespace = if (req.namespace.len == 0) "default" else req.namespace;
        var key_buf: [waiter_pool_mod.MAX_WAITER_KEY]u8 = undefined;
        const key = AwaitKey.build(&key_buf, namespace, action_name, worker_id) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "worker id too long");
            return;
        };

        // The worker's labels, for runs that require some.
        const worker_labels: ?[]const u8 = if (worker_id.len > 0) blk: {
            break :blk if (shard.worker_handler.find(namespace, worker_id)) |w| w.metadata_owned else null;
        } else null;

        // Try to claim a pending run for ANY registered action, not just the first.
        // Workers register multiple actions and cross-shard invocations may create
        // runs for any of them.
        if (tryClaimAnyAction(shard, conn, req, worker_id, worker_labels)) return;

        // No pending work: wait for some. Absent means 30 s; 0 means do not wait.
        const block_ms: u32 = if (req.findOption(.block_ms)) |opt| (opt.asU32() orelse 30_000) else 30_000;
        if (block_ms == 0) return shard.sendOkResponse(conn, req.header.request_id, "");
        const registered = shard.waiter_pool.register(.{
            .kind = .action_await,
            .reply_to = conn.replyTo(),
            .request_id = req.header.request_id,
            .key = key.bytes,
            .min_version = key.lengths,
            .timeout_ms = block_ms,
        });
        if (!registered) return shard.refuseWaitersFull(conn, req.header.request_id);
        conn.response_deferred = true;
    }

    /// Try to claim a pending run for any action type listed in the await request.
    /// Uses hash-based routing: action_invoke is pre-routed by action name, so
    /// pending runs for action "foo" always live on route(hash(ns, "foo")).
    /// O(action_types) instead of O(action_types * shards).
    fn tryClaimAnyAction(shard: *Shard, conn: *Connection, req: Request, worker_id: []const u8, worker_labels: ?[]const u8) bool {
        const namespace = if (req.namespace.len == 0) "default" else req.namespace;
        var it = TaskTypeIterator.init(req.value);
        while (it.next()) |name| {
            const target = resolveActionShard(shard, namespace, name);
            if (target.actions_handler.claimPendingRun(shard.allocator, namespace, name, worker_labels, worker_id)) |task| {
                answerTaskAssignment(shard, conn, req.header.request_id, task);
                return true;
            }
        }
        return false;
    }

    /// Resolve which shard owns a given action name via hash routing.
    /// Returns the target shard (may be self). O(1).
    fn resolveActionShard(shard: *Shard, namespace: []const u8, action_name: []const u8) *Shard {
        const hash = router.hashKeyWithNamespace(namespace, action_name);
        const target = shard.router.route(hash);
        return switch (target) {
            .local => shard,
            .shard => |t| {
                if (shard.peer_shards) |peers| {
                    if (t.shard_id < peers.len) return peers[t.shard_id];
                }
                return shard;
            },
        };
    }

    /// Resolve which shard owns a given worker_id via hash routing.
    /// worker_register is pre-routed by hash(namespace, worker_id), so
    /// the WorkerRecord lives on a potentially different shard than the
    /// action that completed.
    fn resolveWorkerShard(shard: *Shard, namespace: []const u8, worker_id: []const u8) *Shard {
        const ns = if (namespace.len == 0) "default" else namespace;
        const hash = router.hashKeyWithNamespace(ns, worker_id);
        const target = shard.router.route(hash);
        return switch (target) {
            .local => shard,
            .shard => |t| {
                if (shard.peer_shards) |peers| {
                    if (t.shard_id < peers.len) return peers[t.shard_id];
                }
                return shard;
            },
        };
    }

    /// A claimed run's task, built while the run's shard held its lock:
    /// the claimer may be another shard's thread, and nothing it keeps may
    /// point into a run that shard can change or free.
    const ClaimedTask = struct {
        /// The answer body, owned by the claimer (the allocator it passed).
        body: []u8,
        /// The run id, inside `body`.
        run_id: []const u8,
    };

    /// Try to claim a pending run for the given action, its task built
    /// with `allocator`. If worker_labels is provided, only claims runs
    /// whose required labels match. With no memory for the task, nothing
    /// is claimed.
    pub fn claimPendingRun(self: *ActionsHandler, allocator: std.mem.Allocator, namespace: []const u8, action_name: []const u8, worker_labels: ?[]const u8, worker_id: []const u8) ?ClaimedTask {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();

        var it = self.runs.iterator();
        while (it.next()) |entry| {
            const run = entry.value_ptr;
            if (run.status != .pending) continue;
            if (!std.mem.eql(u8, run.action_name_owned, action_name)) continue;
            if (!runIn(run, namespace)) continue;

            // Label check: if run requires labels, worker must have matching labels
            if (run.labels_owned) |required| {
                if (worker_labels) |wl| {
                    if (!labelsMatch(self.allocator, required, wl)) continue;
                } else {
                    // Run requires labels but worker has none — skip
                    continue;
                }
            }

            const body = encodeTaskAssignment(allocator, run, run.attempt + 1) catch {
                log.err("action run '{s}' left pending: no memory to build its task", .{run.run_id_owned});
                return null;
            };
            run.status = .running;
            run.expiry_proposed = false;
            // The claim is off the log: its time is this node's clock until
            // claims are logged with stamps of their own.
            run.started_at_ms = @import("stdx").time.milliTimestamp();
            run.lease_from_ms = run.started_at_ms.?;
            run.lease_ms = null;
            run.attempt += 1;
            if (worker_id.len > 0) {
                if (run.worker_id_owned) |old| self.allocator.free(old);
                run.worker_id_owned = self.allocator.dupe(u8, worker_id) catch null;
            }
            return .{ .body = body, .run_id = body[2..][0..run.run_id_owned.len] };
        }
        return null;
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    fn handleCommand(self: *ActionsHandler, shard: ?*Shard, req: Request) CommandResult {
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        // The registry key is "ns\x00name" ("name" in "default"), so a name
        // holding a NUL could spell another namespace's key.
        switch (op) {
            .action_register, .action_invoke, .action_delete => if (std.mem.indexOfScalar(u8, req.key, 0) != null) {
                return .{ .err = .{ .code = .invalid_request, .message = "action name must not contain NUL" } };
            },
            else => {},
        }
        return switch (op) {
            .action_register => self.handleRegister(shard, req),
            .action_invoke => self.handleInvoke(shard, req),
            .action_list => self.handleList(req),
            .action_list_runs => self.handleListRuns(req),
            .action_delete => self.handleDelete(shard, req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown action opcode" } },
        };
    }

    // ── REGISTER ────────────────────────────────────────────────────────

    const RegisterMeta = struct {
        action_type: ActionType,
        timeout_ms: u32,
        max_retries: u32,
        owner: []const u8,
    };

    /// Parse the action-register wire value:
    ///   `[action_type:u8][timeout_ms:u32][max_retries:u32][owner_len:u16][owner]`
    /// Every field is defaulted so short/legacy values still parse. `owner` is a
    /// borrowed slice into `value`; the caller dupes it if it needs ownership.
    fn parseRegisterValue(value: []const u8) RegisterMeta {
        var m = RegisterMeta{ .action_type = .user, .timeout_ms = DEFAULT_ACTION_TIMEOUT_MS, .max_retries = 3, .owner = "" };
        if (value.len > 0) m.action_type = ActionType.fromU8(value[0]);
        if (value.len >= 5) m.timeout_ms = std.mem.readInt(u32, value[1..5], .little);
        if (value.len >= 9) m.max_retries = std.mem.readInt(u32, value[5..9], .little);
        if (value.len >= 11) {
            const owner_len: usize = std.mem.readInt(u16, value[9..11], .little);
            if (value.len >= 11 + @as(usize, owner_len)) m.owner = value[11 .. 11 + owner_len];
        }
        return m;
    }

    fn handleRegister(self: *ActionsHandler, shard: ?*Shard, req: Request) CommandResult {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();

        const name = req.key;
        const namespace = if (req.namespace.len == 0) "default" else req.namespace;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "action name is required" } };
        }
        if (name.len > MAX_ACTION_NAME_LEN) {
            return .{ .err = .{ .code = .invalid_request, .message = "action name too long" } };
        }
        var kbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const key = defKey(&kbuf, namespace, name) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace + name too long" } };
        };
        if (self.actions.count() >= MAX_ACTIONS and !self.actions.contains(key)) {
            return .{ .err = .{ .code = .internal_error, .message = "action limit reached" } };
        }

        // Wire format: [action_type:u8][timeout_ms:u32][max_retries:u32][owner_len:u16][owner]
        // The entry carries it as sent; the applier stores the record from
        // it, created at the entry's stamp, on the leader, followers and
        // restart alike.
        const value = req.value;
        var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = ns_keys.qualifyKey(&qbuf, namespace, name) catch {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace + name too long" } };
        };
        if (shard) |s| {
            const proposed = persistence.proposeEntry(s, .action_register, Flags.NONE, req.namespace, qkey, value) catch |err| {
                return .{ .err = persistence.failureError(err, "action not persisted") };
            };
            return .{ .parked = proposed };
        }
        // Without a shard (unit tests): applied here, answered the same.
        self.local_result = .{};
        self.replayRegister(qkey, value, wallNs(), &self.local_result) catch
            return .{ .err = .{ .code = .committed_not_applied, .message = persistence.COMMITTED_NOT_APPLIED } };
        return registeredFrom(&self.local_result, &self.local_ver_buf);
    }

    /// The register applied: the action's name and the version it stored.
    /// The answer points into `result` and `ver_buf`.
    fn registeredFrom(result: *const Result, ver_buf: *[12]u8) CommandResult {
        var r = result.reader();
        const version = r.int(u32) orelse 1;
        const name = r.slice() orelse "";
        return .{ .action_registered = .{
            .name = name,
            .version = std.fmt.bufPrint(ver_buf, "{d}", .{version}) catch "1",
        } };
    }

    // ── INVOKE ──────────────────────────────────────────────────────────

    fn handleInvoke(self: *ActionsHandler, shard: ?*Shard, req: Request) CommandResult {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();

        const action_name = req.key;

        if (action_name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "action name is required" } };
        }

        // Check action exists, in this namespace
        var kbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const key = defKey(&kbuf, req.namespace, action_name) orelse {
            return .{ .err = .{ .code = .not_found, .message = "action not found" } };
        };
        if (!self.actions.contains(key)) {
            return .{ .err = .{ .code = .not_found, .message = "action not found" } };
        }

        const parsed_value = parseInvokeValue(self.allocator, req.value) catch |err| return .{ .err = switch (err) {
            error.OutOfMemory => .{ .code = .internal_error, .message = "out of memory" },
            error.Empty => .{ .code = .invalid_request, .message = "invoke value is empty: it starts with a has_labels byte" },
            error.BadFlag => .{ .code = .invalid_request, .message = "invoke has_labels must be 0 or 1" },
            error.Truncated => .{ .code = .invalid_request, .message = "invoke labels run past the end of the value" },
            error.LabelsNotObject => .{ .code = .invalid_request, .message = "invoke labels must be a JSON object" },
        } };

        // Generate run ID with embedded partition bits
        var run_id_buf: [32]u8 = undefined;
        const run_id_str = if (shard) |s| blk: {
            const partition_id = s.router.keyToPartitionNs(req.namespace, action_name);
            break :blk s.run_id_gen.next(.action, partition_id, &run_id_buf);
        } else blk: {
            // Monotonic counter so concurrent null-shard invocations (tests)
            // don't collide when called within the same millisecond.
            const seq = self.null_shard_seq.fetchAdd(1, .monotonic);
            break :blk std.fmt.bufPrint(&run_id_buf, "act-{d}-{d}", .{ @import("stdx").time.milliTimestamp(), seq }) catch unreachable;
        };

        // The applier creates the run from the entry.
        var value_buf: [MAX_INVOKE_VALUE]u8 = undefined;
        const value = encodeInvokeValue(&value_buf, homeOf(req.namespace), action_name, parsed_value.input, parsed_value.labels, null, null) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "action input too large" } };
        };
        if (shard) |s| {
            const proposed = persistence.proposeEntry(s, .action_invoke, Flags.NONE, req.namespace, run_id_str, value) catch |err| {
                return .{ .err = persistence.failureError(err, "action not persisted") };
            };
            return .{ .parked = proposed };
        }
        self.local_result = .{};
        self.replayInvoke(run_id_str, value, wallNs(), &self.local_result) catch
            return .{ .err = .{ .code = .committed_not_applied, .message = persistence.COMMITTED_NOT_APPLIED } };
        if (self.local_result.refused()) |r| return .{ .err = .{ .code = .internal_error, .message = r.refusal.message } };
        return invokedFrom(&self.local_result);
    }

    /// The invoke applied: the run it created, and how many runs the shard
    /// held then. The answer points into `result`.
    fn invokedFrom(result: *const Result) CommandResult {
        var r = result.reader();
        const run_id = r.slice() orelse "";
        return .{ .action_invoked = .{ .run_id = run_id, .queue_position = r.int(u32) orelse 0 } };
    }

    // ── STATUS ──────────────────────────────────────────────────────────

    const StatusResponse = table_run.op("action.status").response;

    comptime {
        // action.status answers with the run's own status values.
        for (@typeInfo(ActionRunStatus).@"enum".field_names, @typeInfo(ActionRunStatus).@"enum".field_values) |name, value| {
            if (@backingInt(@field(action_ops.RunStatus, name)) != value) @compileError("action.status RunStatus." ++ name ++ " differs from the run's");
        }
    }

    pub const Error = error{ NotFound, OutOfMemory, StoredTooLong };

    pub fn refusal(err: Error) table_run.Refused {
        return switch (err) {
            error.NotFound => .{ .status = .not_found, .refusal = .{ .reason = .not_found, .ran = .no, .message = "not found: no such run" } },
            error.OutOfMemory => .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "internal error: out of memory" } },
            error.StoredTooLong => .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "internal error: run output or error longer than status can carry" } },
        };
    }

    /// A run's status, times and attempts, its output once it completed,
    /// and the error its last attempt failed with.
    fn runStatus(shard: *Shard, _: *Connection, req: Request) Error!table_run.Outcome(StatusResponse) {
        return .{ .answer = try shard.actions_handler.answerStatus(req.namespace, req.key) };
    }

    fn answerStatus(self: *ActionsHandler, namespace: []const u8, run_id: []const u8) Error!StatusResponse.Value {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();
        const run = self.runs.getPtr(run_id) orelse return error.NotFound;
        if (!runIn(run, namespace)) return error.NotFound;
        const buf = self.status_buf orelse blk: {
            const b = try self.allocator.alloc(u8, action_ops.MAX_OUTPUT + action_ops.MAX_ERROR + action_ops.MAX_WORKER_ID);
            self.status_buf = b;
            break :blk b;
        };
        // Completes and fails store at most these lengths today; a longer
        // value is refused, never cut short.
        var used: usize = 0;
        const output: ?[]const u8 = if (run.status == .completed) blk: {
            const r = run.result_owned orelse "";
            if (r.len > action_ops.MAX_OUTPUT) return error.StoredTooLong;
            @memcpy(buf[0..r.len], r);
            used = r.len;
            break :blk buf[0..r.len];
        } else null;
        const error_message: ?[]const u8 = if (run.error_owned) |e| if (e.len == 0) null else blk: {
            if (e.len > action_ops.MAX_ERROR) return error.StoredTooLong;
            @memcpy(buf[used..][0..e.len], e);
            defer used += e.len;
            break :blk buf[used..][0..e.len];
        } else null;
        const worker_id: ?[]const u8 = if (run.worker_id_owned) |w| blk: {
            if (w.len > action_ops.MAX_WORKER_ID) return error.StoredTooLong;
            @memcpy(buf[used..][0..w.len], w);
            break :blk buf[used..][0..w.len];
        } else null;
        return .{
            .status = @fromBackingInt(@intCast(@backingInt(run.status))),
            .created_at_ms = run.created_at_ms,
            .started_at_ms = run.started_at_ms,
            .completed_at_ms = run.completed_at_ms,
            .attempt = run.attempt,
            .output = output,
            .error_message = error_message,
            .worker_id = worker_id,
        };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *ActionsHandler, req: Request) CommandResult {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();

        const namespace = if (req.namespace.len == 0) "default" else req.namespace;

        const data = self.serializeActionList(namespace) catch {
            return .{ .err = .{ .code = .internal_error, .message = "action list serialization failed" } };
        };

        return .{ .action_list_result = .{
            .data = data,
            .cursor = null,
        } };
    }

    // ── LIST RUNS ───────────────────────────────────────────────────────

    /// List all runs for a specific action.
    /// key = action_name (or empty for all runs on this shard)
    /// Response: action_list_result with binary data:
    ///   [count:u32] ([run_id_len:u16][run_id][action_name_len:u16][action_name]
    ///                [status:u8][created_at:i64][has_started:u8][started_at?:i64]
    ///                [has_completed:u8][completed_at?:i64])*
    fn handleListRuns(self: *ActionsHandler, req: Request) CommandResult {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();

        const action_filter = req.key;

        // Parse limit from value field (u32 LE, default 100)
        const limit: u32 = if (req.value.len >= 4)
            std.mem.readInt(u32, req.value[0..4], .little)
        else
            100;

        // Calculate total size
        var count: u32 = 0;
        var total_size: usize = 4; // count header
        var it = self.runs.iterator();
        while (it.next()) |entry| {
            const run = entry.value_ptr;
            if (action_filter.len > 0 and !std.mem.eql(u8, run.action_name_owned, action_filter)) continue;
            if (!runIn(run, req.namespace)) continue;
            if (count >= limit) break;
            count += 1;
            total_size += 2 + run.run_id_owned.len; // run_id
            total_size += 2 + run.action_name_owned.len; // action_name
            total_size += 1; // status
            total_size += 8; // created_at
            total_size += 1 + if (run.started_at_ms != null) @as(usize, 8) else 0;
            total_size += 1 + if (run.completed_at_ms != null) @as(usize, 8) else 0;
        }

        const buf = self.allocator.alloc(u8, total_size) catch {
            return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        };
        var off: usize = 0;

        std.mem.writeInt(u32, buf[off..][0..4], count, .little);
        off += 4;

        var emitted: u32 = 0;
        var it2 = self.runs.iterator();
        while (it2.next()) |entry| {
            const run = entry.value_ptr;
            if (action_filter.len > 0 and !std.mem.eql(u8, run.action_name_owned, action_filter)) continue;
            if (!runIn(run, req.namespace)) continue;
            if (emitted >= limit) break;
            emitted += 1;

            // run_id
            std.mem.writeInt(u16, buf[off..][0..2], @intCast(run.run_id_owned.len), .little);
            off += 2;
            @memcpy(buf[off .. off + run.run_id_owned.len], run.run_id_owned);
            off += run.run_id_owned.len;

            // action_name
            std.mem.writeInt(u16, buf[off..][0..2], @intCast(run.action_name_owned.len), .little);
            off += 2;
            @memcpy(buf[off .. off + run.action_name_owned.len], run.action_name_owned);
            off += run.action_name_owned.len;

            // status
            buf[off] = @backingInt(run.status);
            off += 1;

            // created_at
            std.mem.writeInt(i64, buf[off..][0..8], run.created_at_ms, .little);
            off += 8;

            // started_at (optional)
            if (run.started_at_ms) |v| {
                buf[off] = 1;
                off += 1;
                std.mem.writeInt(i64, buf[off..][0..8], v, .little);
                off += 8;
            } else {
                buf[off] = 0;
                off += 1;
            }

            // completed_at (optional)
            if (run.completed_at_ms) |v| {
                buf[off] = 1;
                off += 1;
                std.mem.writeInt(i64, buf[off..][0..8], v, .little);
                off += 8;
            } else {
                buf[off] = 0;
                off += 1;
            }
        }

        return .{ .action_list_result = .{
            .data = buf,
            .cursor = null,
        } };
    }

    /// ShardWalker LocalScanFn for action_list — returns action names
    /// from one shard's ActionsHandler registry.
    fn localScanActions(
        ctx: *anyopaque,
        namespace: []const u8,
        _: []const u8, // filter
        _: ?[]const u8, // cursor
        _: u32, // limit
    ) dispatcher_mod.NameWalker.ScanResult {
        const handler: *ActionsHandler = @ptrCast(@alignCast(ctx));
        handler.runs_mu.lock();
        defer handler.runs_mu.unlock();
        // Sized under the lock, so an insert can't outgrow it mid-scan.
        const name_buf = dispatcher_mod.scanScratch(handler.actions.count());

        const effective_ns = if (namespace.len == 0) "default" else namespace;

        var count: usize = 0;
        var it = handler.actions.iterator();
        while (it.next()) |entry| {
            if (count >= name_buf.len) break;
            if (!std.mem.eql(u8, entry.value_ptr.namespace_owned, effective_ns)) continue;
            name_buf[count] = entry.value_ptr.name_owned;
            count += 1;
        }

        return .{ .items = name_buf[0..count], .next_cursor = null };
    }

    // ── DELETE ───────────────────────────────────────────────────────────

    fn handleDelete(self: *ActionsHandler, shard: ?*Shard, req: Request) CommandResult {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();

        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "action name is required" } };
        }

        var qbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = defKey(&qbuf, req.namespace, name) orelse {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace + name too long" } };
        };
        if (self.actions.contains(qkey)) {
            if (shard) |s| {
                const proposed = persistence.proposeEntry(s, .action_delete, Flags.TOMBSTONE, req.namespace, qkey, "") catch |err| {
                    return .{ .err = persistence.failureError(err, "action not persisted") };
                };
                return .{ .parked = proposed };
            }
            self.replayDelete(qkey);
            return .{ .action_deleted = {} };
        }

        // Idempotent — deleting non-existent action is OK
        return .{ .action_deleted = {} };
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    pub fn actionCount(self: *const ActionsHandler) usize {
        return self.actions.count();
    }

    pub fn runCount(self: *const ActionsHandler) usize {
        return self.runs.count();
    }

    // ── Action Task Command Handlers ───────────────────────────────────

    /// A run nothing moves on from.
    fn finished(status: ActionRunStatus) bool {
        return switch (status) {
            .completed, .failed, .cancelled, .timed_out => true,
            .pending, .running => false,
        };
    }

    /// A report the run already shows (a resend) is answered ok; "shows"
    /// is the status alone, so of two different reports the first applied
    /// stands. A report for a finished run, or from anyone but the worker
    /// holding a running run, is stale and refused, so a late fail can't
    /// put a finished or re-dispatched run back to pending. A pending run
    /// takes reports: claims are off the log, so a run claimed before a
    /// leader change is pending here. `replayUpdateRun` separately leaves a
    /// finished run alone on every replica.
    ///
    /// Not caught until reports carry the attempt they answer: a late
    /// report from the same worker after it claimed the run again, and two
    /// reports in flight at once (both pass here; the second fail applies).
    fn staleReport(run: *const RunRecord, worker_id: []const u8, reported: ActionRunStatus) ?TaskOutcome {
        switch (run.status) {
            .running => {
                const holder = run.worker_id_owned orelse return null;
                if (std.mem.eql(u8, holder, worker_id)) return null;
                return .{ .refused = .{ .status = .conflict, .reason = .stale_report, .message = "stale report: another worker is running this run now" } };
            },
            .pending => return if (reported == .pending) .repeat else null,
            .completed, .failed, .cancelled, .timed_out => {
                if (run.status == reported) return .repeat;
                return .{ .refused = .{ .status = .conflict, .reason = .stale_report, .message = "stale report: the run has finished" } };
            },
        }
    }

    /// Complete a task. key = worker_id.
    /// value = [action_name_len:u16][action_name][task_id_len:u16][task_id]
    ///         [outcome_len:u16][outcome][result_len:u16][result]
    fn handleActionComplete(self: *ActionsHandler, shard: ?*Shard, req: Request) TaskOutcome {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();

        const value = req.value;
        var offset: usize = 0;

        // Parse action_name (skip it — we use task_id to find the run)
        if (offset + 2 > value.len) return .{ .refused = .{ .status = .bad_request, .message = "invalid value format" } };
        const aname_len: usize = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2 + aname_len;

        // Parse task_id
        if (offset + 2 > value.len) return .{ .refused = .{ .status = .bad_request, .message = "invalid value format" } };
        const tid_len: usize = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2;
        if (offset + tid_len > value.len) return .{ .refused = .{ .status = .bad_request, .message = "invalid value format" } };
        const task_id = value[offset .. offset + tid_len];
        offset += tid_len;

        // Parse outcome
        if (offset + 2 > value.len) return .{ .refused = .{ .status = .bad_request, .message = "invalid value format" } };
        const outcome_len: usize = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2;
        if (offset + outcome_len > value.len) return .{ .refused = .{ .status = .bad_request, .message = "invalid value format" } };
        const outcome_str = value[offset .. offset + outcome_len];
        offset += outcome_len;

        // Parse result
        var result_data: []const u8 = "";
        if (offset + 2 <= value.len) {
            const rlen: usize = std.mem.readInt(u16, value[offset..][0..2], .little);
            offset += 2;
            if (offset + rlen <= value.len) {
                result_data = value[offset .. offset + rlen];
            }
        }

        // The applier updates the run from the entry.
        const run = self.runs.getPtr(task_id) orelse return .{ .refused = .{ .status = .not_found, .message = "run not found" } };
        if (!runIn(run, req.namespace)) return .{ .refused = .{ .status = .not_found, .message = "run not found" } };
        if (staleReport(run, req.key, .completed)) |o| return o;
        const worker_id: ?[]const u8 = if (req.key.len > 0) req.key else run.worker_id_owned;
        return self.applyRunUpdate(shard, req.namespace, task_id, .{
            .status = .completed,
            .started_at_ms = run.started_at_ms,
            .result = result_data,
            .worker_id = worker_id,
            .outcome = outcome_str,
            .error_message = "",
        });
    }

    /// Touch (extend lease on) a running task. key = worker_id.
    /// value = [action_name_len:u16][action_name][task_id_len:u16][task_id]
    fn handleActionTouch(self: *ActionsHandler, req: Request) ?TaskError {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();

        const value = req.value;
        var offset: usize = 0;

        // Parse action_name (skip)
        if (offset + 2 > value.len) return .{ .status = .bad_request, .message = "invalid value format" };
        const aname_len: usize = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2 + aname_len;

        // Parse task_id
        if (offset + 2 > value.len) return .{ .status = .bad_request, .message = "invalid value format" };
        const tid_len: usize = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2;
        if (offset + tid_len > value.len) return .{ .status = .bad_request, .message = "invalid value format" };
        const task_id = value[offset .. offset + tid_len];
        offset += tid_len;
        // How long the lease should run from now, at least the action's
        // timeout; 0 or absent asks for just that.
        const extend_ms: u32 = if (offset + 4 <= value.len) std.mem.readInt(u32, value[offset..][0..4], .little) else 0;
        if (extend_ms > MAX_TOUCH_EXTEND_MS) return .{ .status = .bad_request, .message = std.fmt.comptimePrint("bad request: extend_ms is at most {d}", .{MAX_TOUCH_EXTEND_MS}) };

        if (self.runs.getPtr(task_id)) |run| {
            if (!runIn(run, req.namespace)) return .{ .status = .not_found, .message = "run not found" };
            if (run.status == .running) {
                run.lease_from_ms = @import("stdx").time.milliTimestamp();
                run.lease_ms = if (extend_ms > 0) extend_ms else null;
                return null; // success
            }
            return .{ .status = .conflict, .message = "run not in running state" };
        }
        return .{ .status = .not_found, .message = "run not found" };
    }

    /// Fail a task. key = worker_id.
    /// value = [action_name_len:u16][action_name][task_id_len:u16][task_id][retry:u8][error_message...]
    fn handleActionFail(self: *ActionsHandler, shard: ?*Shard, req: Request) TaskOutcome {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();

        const value = req.value;
        var offset: usize = 0;

        // Parse action_name (skip)
        if (offset + 2 > value.len) return .{ .refused = .{ .status = .bad_request, .message = "invalid value format" } };
        const aname_len: usize = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2 + aname_len;

        // Parse task_id
        if (offset + 2 > value.len) return .{ .refused = .{ .status = .bad_request, .message = "invalid value format" } };
        const tid_len: usize = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2;
        if (offset + tid_len > value.len) return .{ .refused = .{ .status = .bad_request, .message = "invalid value format" } };
        const task_id = value[offset .. offset + tid_len];
        offset += tid_len;

        // Parse retry flag
        if (offset >= value.len) return .{ .refused = .{ .status = .bad_request, .message = "invalid value format" } };
        const retry = value[offset] == 1;
        offset += 1;

        // Remaining bytes are error message
        const error_message = if (offset < value.len) value[offset..] else "";

        // The applier updates the run from the entry.
        const run = self.runs.getPtr(task_id) orelse return .{ .refused = .{ .status = .not_found, .message = "run not found" } };
        if (!runIn(run, req.namespace)) return .{ .refused = .{ .status = .not_found, .message = "run not found" } };
        const again = retry and run.attempt < run.max_retries;
        if (staleReport(run, req.key, if (again) .pending else .failed)) |o| return o;
        const worker_id: ?[]const u8 = if (req.key.len > 0) req.key else run.worker_id_owned;
        if (again) {
            // Back to pending for another attempt (within the limit).
            return self.applyRunUpdate(shard, req.namespace, task_id, .{
                .status = .pending,
                .started_at_ms = null,
                .result = "",
                .worker_id = worker_id,
                .outcome = "",
                .error_message = error_message,
            });
        }
        return self.applyRunUpdate(shard, req.namespace, task_id, .{
            .status = .failed,
            .started_at_ms = run.started_at_ms,
            .result = "",
            .worker_id = worker_id,
            .outcome = "",
            .error_message = error_message,
        });
    }

    // ── Serialization ───────────────────────────────────────────────────

    /// Wire format (scan format matching CLI expectations):
    ///   [count:u32] ([key_len:u16][key][value_len:u32][value])* [has_more:u8] [cursor_len:u16]
    /// Key = action name, Value = [type:u8][version:u32][enabled:u8]
    fn serializeActionList(self: *ActionsHandler, namespace: []const u8) ![]u8 {
        const value_size: usize = 1 + 4 + 1; // type + version + enabled
        var total_size: usize = 4; // count
        var entry_count: u32 = 0;
        var it = self.actions.iterator();
        while (it.next()) |entry| {
            if (!std.mem.eql(u8, entry.value_ptr.namespace_owned, namespace)) continue;
            total_size += 2 + entry.value_ptr.name_owned.len + 4 + value_size;
            entry_count += 1;
        }
        total_size += 1 + 2; // has_more(u8) + cursor_len(u16)

        const buf = try self.allocator.alloc(u8, total_size);
        errdefer self.allocator.free(buf);
        var offset: usize = 0;

        std.mem.writeInt(u32, buf[offset..][0..4], entry_count, .little);
        offset += 4;

        var it2 = self.actions.iterator();
        while (it2.next()) |entry| {
            const rec = entry.value_ptr;
            if (!std.mem.eql(u8, rec.namespace_owned, namespace)) continue;
            // key_len + key
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(rec.name_owned.len), .little);
            offset += 2;
            @memcpy(buf[offset .. offset + rec.name_owned.len], rec.name_owned);
            offset += rec.name_owned.len;
            // value_len + value (type + version + enabled)
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(value_size), .little);
            offset += 4;
            buf[offset] = @backingInt(rec.action_type);
            offset += 1;
            std.mem.writeInt(u32, buf[offset..][0..4], rec.version, .little);
            offset += 4;
            buf[offset] = if (rec.enabled) 1 else 0;
            offset += 1;
        }

        // No more data, no cursor
        buf[offset] = 0; // has_more = false
        offset += 1;
        std.mem.writeInt(u16, buf[offset..][0..2], 0, .little); // cursor_len = 0
        offset += 2;

        return buf;
    }

    // ── Free Result ─────────────────────────────────────────────────────

    pub fn freeResult(self: *ActionsHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .action_list_result => |r| self.allocator.free(r.data),
            .action_invoked => |r| {
                _ = r;
            },
            else => {},
        }
    }

    // ── Internal API (for WorkflowHandler) ──────────────────────────────

    /// Result of an internal action invocation.
    pub const InternalRunResult = struct {
        status: ActionRunStatus,
        output: ?[]const u8,
        outcome: ?[]const u8 = null,
    };

    /// An invoke a workflow proposed: the run's id and the entry's index.
    pub const Invoked = struct { id: []const u8, index: u64 };

    /// Invoke an action programmatically (used by WorkflowHandler): the
    /// invoke is proposed and the run exists once it applies. The id is
    /// written into `id_buf`; null if the action doesn't exist or the
    /// invoke could not be proposed.
    pub fn invokeByName(self: *ActionsHandler, shard: *Shard, namespace: []const u8, action_name: []const u8, input: ?[]const u8, caller_run_id: ?[]const u8, caller_workflow_name: ?[]const u8, id_buf: *[32]u8) ?Invoked {
        // Check action exists, in the workflow's namespace
        var kbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        if (!self.actions.contains(defKey(&kbuf, namespace, action_name) orelse return null)) return null;

        // Generate run ID with embedded partition bits
        const partition_id = shard.router.keyToPartitionNs(namespace, action_name);
        const run_id_str = shard.run_id_gen.next(.action, partition_id, id_buf);

        // The applier creates the run from the entry, exactly as a client
        // invoke does; the caller fields mark it as workflow-driven.
        var value_buf: [MAX_INVOKE_VALUE]u8 = undefined;
        const value = encodeInvokeValue(&value_buf, homeOf(namespace), action_name, input orelse "", null, caller_run_id, caller_workflow_name) orelse return null;
        const index = proposeInvoke(shard, homeOf(namespace), run_id_str, value) orelse return null;
        return .{ .id = run_id_str, .index = index };
    }

    /// Propose an invoke; the applier creates the run and wakes the
    /// workers waiting for it. Nothing waits for the commit: a caller that
    /// needs the run finds it once it applies.
    fn proposeInvoke(shard: *Shard, namespace: []const u8, run_id: []const u8, value: []const u8) ?u64 {
        const proposed = persistence.proposeEntry(shard, .action_invoke, Flags.NONE, namespace, run_id, value) catch |err| {
            if (!shard.quietHalted(err)) log.err("shard {d}: action run {s} not started: {s}", .{ shard.id, run_id, @errorName(err) });
            return null;
        };
        return proposed.index;
    }

    /// A workflow on another shard asked this shard to start a run it will
    /// park on: `[run_id_len:u16][run_id][invoke value]`. Proposed here, on
    /// the owning thread; the run exists once the invoke applies.
    pub fn startRunFromInbox(_: *ActionsHandler, shard: *Shard, bytes: []const u8) void {
        if (bytes.len < 2) return;
        const rid_len: usize = std.mem.readInt(u16, bytes[0..2], .little);
        if (2 + rid_len > bytes.len) return;
        const run_id = bytes[2 .. 2 + rid_len];
        const value = bytes[2 + rid_len ..];
        if (proposeInvoke(shard, invokeNamespace(value), run_id, value) == null) {
            log.err("shard {d}: action run {s} asked for by a workflow on another shard could not be started; that workflow run stays parked", .{ shard.id, run_id });
        }
    }

    /// Encode the inbox payload `startRunFromInbox` consumes. Heap-allocated
    /// with `allocator`; the receiving shard frees it.
    pub fn encodeStartRunMessage(allocator: Allocator, namespace: []const u8, run_id: []const u8, action_name: []const u8, input: []const u8, caller_run_id: ?[]const u8, caller_workflow_name: ?[]const u8) ?[]u8 {
        var value_buf: [MAX_INVOKE_VALUE]u8 = undefined;
        const value = encodeInvokeValue(&value_buf, homeOf(namespace), action_name, input, null, caller_run_id, caller_workflow_name) orelse return null;
        const out = allocator.alloc(u8, 2 + run_id.len + value.len) catch return null;
        std.mem.writeInt(u16, out[0..2], @intCast(run_id.len), .little);
        @memcpy(out[2 .. 2 + run_id.len], run_id);
        @memcpy(out[2 + run_id.len ..], value);
        return out;
    }

    const RunUpdate = struct {
        status: ActionRunStatus,
        /// When the worker claimed the run. The claim happens off the log,
        /// so its time is the claiming node's clock, carried here; it moves
        /// to the claim's own stamp when claims are logged.
        started_at_ms: ?i64,
        result: []const u8,
        worker_id: ?[]const u8,
        outcome: []const u8,
        error_message: []const u8,
    };

    /// Time out running runs past their lease: the longer of the action's
    /// `timeout_ms` and the last touch's `extend_ms`, counted from the claim
    /// or that touch, on this node's clock; an action with `timeout_ms` 0
    /// never times out. A
    /// run with attempts left goes back to pending, as a failure asking for
    /// a retry does, and one with none is timed out. Proposed as ordinary
    /// run updates, so every replica applies them; the leader runs this,
    /// since only it holds claims. Returns how many it proposed.
    pub fn expireRuns(self: *ActionsHandler, shard: ?*Shard, now_ms: i64) usize {
        const Expiry = struct { run_id: []u8, namespace: []u8, update: RunUpdate, msg: [48]u8 };
        var due: [32]Expiry = undefined;
        var n: usize = 0;
        {
            self.runs_mu.lock();
            defer self.runs_mu.unlock();
            var it = self.runs.iterator();
            while (it.next()) |entry| {
                if (n == due.len) break;
                const run = entry.value_ptr;
                if (run.status != .running or run.expiry_proposed) continue;
                // A deleted action's runs end on the default timeout.
                var kbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
                const action_timeout: u32 = if (defKey(&kbuf, run.namespace_owned, run.action_name_owned)) |key|
                    (if (self.actions.get(key)) |a| a.timeout_ms else DEFAULT_ACTION_TIMEOUT_MS)
                else
                    DEFAULT_ACTION_TIMEOUT_MS;
                if (action_timeout == 0) continue;
                // A touch can lengthen the lease past the action's timeout,
                // never shorten it.
                const lease_ms = @max(run.lease_ms orelse 0, action_timeout);
                if (now_ms - run.lease_from_ms < lease_ms) continue;
                const run_id = self.allocator.dupe(u8, run.run_id_owned) catch break;
                const namespace = self.allocator.dupe(u8, run.namespace_owned) catch {
                    self.allocator.free(run_id);
                    break;
                };
                due[n] = .{ .run_id = run_id, .namespace = namespace, .msg = undefined, .update = .{
                    .status = if (run.attempt < run.max_retries) .pending else .timed_out,
                    .started_at_ms = null,
                    .result = "",
                    .worker_id = null,
                    .outcome = "",
                    .error_message = "",
                } };
                due[n].update.error_message = std.fmt.bufPrint(&due[n].msg, "timed out after {d} ms", .{lease_ms}) catch "timed out";
                run.expiry_proposed = true;
                n += 1;
            }
        }
        for (due[0..n]) |*d| {
            defer self.allocator.free(d.run_id);
            defer self.allocator.free(d.namespace);
            if (self.applyRunUpdate(shard, d.namespace, d.run_id, d.update) == .refused) self.clearExpiry(d.run_id);
        }
        return n;
    }

    /// Let a run's timeout be proposed again: its proposal was refused.
    fn clearExpiry(self: *ActionsHandler, run_id: []const u8) void {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();
        if (self.runs.getPtr(run_id)) |run| run.expiry_proposed = false;
    }

    /// This node stopped leading: a timeout it proposed may never apply,
    /// and whoever leads next decides afresh.
    pub fn forgetExpiries(self: *ActionsHandler) void {
        self.runs_mu.lock();
        defer self.runs_mu.unlock();
        var it = self.runs.valueIterator();
        while (it.next()) |run| run.expiry_proposed = false;
    }

    /// Encode a run update and propose it (or, without a shard, apply it).
    fn applyRunUpdate(self: *ActionsHandler, shard: ?*Shard, namespace: []const u8, run_id: []const u8, u: RunUpdate) TaskOutcome {
        var value_buf: [65536]u8 = undefined;
        const value = encodeRunUpdateValue(&value_buf, u) orelse return .{ .refused = .{ .status = .bad_request, .message = "run update too large" } };
        const s = shard orelse {
            self.replayUpdateRun(run_id, value, wallNs()) catch {};
            return .applied;
        };
        // Proposed, not waited for: the dispatcher parks the worker on it.
        const proposed = persistence.proposeEntry(s, .action_update_run, Flags.NONE, namespace, run_id, value) catch |err| {
            return .{ .refused = .{ .status = persistence.failureStatus(err), .message = persistence.failureMessage(err, "run update not persisted") } };
        };
        return .{ .parked = proposed };
    }

    /// Check the status and result of an action run.
    /// Thread-safe: protected by runs_mu for cross-shard access.
    pub fn getRunResult(self: *ActionsHandler, run_id: []const u8) ?InternalRunResult {
        const run = self.runs.get(run_id) orelse return null;
        return .{
            .status = run.status,
            .output = run.result_owned,
            .outcome = run.outcome_owned,
        };
    }

    // ═══════════════════════════════════════════════════════════════════
    // Entry encoders — the layouts the appliers decode
    // ═══════════════════════════════════════════════════════════════════

    fn putLenPrefixed(buf: []u8, off: *usize, comptime L: type, bytes: []const u8) bool {
        const n = @sizeOf(L);
        if (off.* + n + bytes.len > buf.len) return false;
        std.mem.writeInt(L, buf[off.*..][0..n], @intCast(bytes.len), .little);
        off.* += n;
        @memcpy(buf[off.* .. off.* + bytes.len], bytes);
        off.* += bytes.len;
        return true;
    }

    /// Invoke entry value: [action_name_len:u16][action_name][status:u8]
    ///   [input_len:u32][input][labels_len:u32][labels]
    ///   [caller_run_id_len:u16][caller_run_id][caller_wf_len:u16][caller_wf]
    ///   [namespace_len:u16][namespace]
    pub fn encodeInvokeValue(value_buf: []u8, namespace: []const u8, action_name: []const u8, input: []const u8, labels: ?[]const u8, caller_run_id: ?[]const u8, caller_workflow_name: ?[]const u8) ?[]const u8 {
        var off: usize = 0;
        if (!putLenPrefixed(value_buf, &off, u16, action_name)) return null;
        if (off + 1 > value_buf.len) return null;
        value_buf[off] = @backingInt(ActionRunStatus.pending);
        off += 1;
        if (!putLenPrefixed(value_buf, &off, u32, input)) return null;
        if (!putLenPrefixed(value_buf, &off, u32, labels orelse "")) return null;
        if (!putLenPrefixed(value_buf, &off, u16, caller_run_id orelse "")) return null;
        if (!putLenPrefixed(value_buf, &off, u16, caller_workflow_name orelse "")) return null;
        if (!putLenPrefixed(value_buf, &off, u16, namespace)) return null;
        return value_buf[0..off];
    }

    /// Run update value: [status:u8][has_started:u8][started_at:i64]?
    ///   [result_len:u32][result][worker_id_len:u16][worker_id][outcome_len:u16][outcome][error_len:u16][error]
    /// A run that finishes finishes at the entry's stamp.
    fn encodeRunUpdateValue(value_buf: []u8, u: RunUpdate) ?[]const u8 {
        var off: usize = 0;
        if (off + 1 + 9 > value_buf.len) return null;
        value_buf[off] = @backingInt(u.status);
        off += 1;
        if (u.started_at_ms) |sa| {
            value_buf[off] = 1;
            off += 1;
            std.mem.writeInt(i64, value_buf[off..][0..8], sa, .little);
            off += 8;
        } else {
            value_buf[off] = 0;
            off += 1;
        }
        if (!putLenPrefixed(value_buf, &off, u32, u.result)) return null;
        if (!putLenPrefixed(value_buf, &off, u16, u.worker_id orelse "")) return null;
        if (!putLenPrefixed(value_buf, &off, u16, u.outcome)) return null;
        if (!putLenPrefixed(value_buf, &off, u16, u.error_message)) return null;
        return value_buf[0..off];
    }

    // ═══════════════════════════════════════════════════════════════════
    // Appliers — the only code that mutates actions and runs, from an entry
    // (live, replicated, or replayed at boot)
    // ═══════════════════════════════════════════════════════════════════

    /// Register this handler's entry types with the ReplayRegistry.
    pub fn registerReplay(self: *ActionsHandler, registry: *persistence.ReplayRegistry) void {
        registry.register(.action_register, @ptrCast(self), replayEntryThunk);
        registry.register(.action_delete, @ptrCast(self), replayEntryThunk);
        registry.register(.action_invoke, @ptrCast(self), replayEntryThunk);
        registry.register(.action_update_run, @ptrCast(self), replayEntryThunk);
    }

    /// Thunk for ReplayRegistry → ActionsHandler.replayEntry.
    fn replayEntryThunk(ctx: *anyopaque, e: *const Entry, out: *Result) persistence.ApplyError!void {
        const self: *ActionsHandler = @ptrCast(@alignCast(ctx));
        return self.replayEntry(e, out);
    }

    /// Dispatch a replayed entry to the appropriate replay handler.
    pub fn replayEntry(self: *ActionsHandler, e: *const Entry, out: *Result) persistence.ApplyError!void {
        const etype: EntryType = @fromBackingInt(@intCast(e.header.entry_type));
        // Workflows on other shard threads read the runs map under this
        // lock. Nothing holds it across an apply, so taking it here cannot
        // nest.
        self.runs_mu.lock();
        defer self.runs_mu.unlock();
        const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
        return switch (etype) {
            .action_register => self.replayRegister(cmd.key, cmd.value, e.header.timestamp_ns, out),
            .action_delete => self.replayDelete(cmd.key),
            .action_invoke => self.replayInvoke(cmd.key, cmd.value, e.header.timestamp_ns, out),
            .action_update_run => self.replayUpdateRun(cmd.key, cmd.value, e.header.timestamp_ns),
            else => error.Halt,
        };
    }

    /// Rebuild an ActionRecord from a persisted register entry.
    /// Key may be namespace-qualified (ns\x00name) or plain name (default namespace).
    pub fn replayRegister(self: *ActionsHandler, key: []const u8, value: []const u8, stamp_ns: u64, out: *Result) persistence.ApplyError!void {
        // Extract namespace from qualified key
        var namespace: []const u8 = "default";
        var name = key;
        if (std.mem.indexOfScalar(u8, key, ns_keys.NAMESPACE_SEPARATOR)) |sep| {
            namespace = key[0..sep];
            name = key[sep + 1 ..];
        }

        const version: u32 = if (self.actions.get(key)) |old| old.version + 1 else 1;
        // The client's register value, so owner, timeout and retries
        // replay from it.
        const meta = parseRegisterValue(value);

        // Remove old entry if re-registering
        if (self.actions.fetchRemove(key)) |old| self.freeAction(old.value);

        const owned_key = self.allocator.dupe(u8, key) catch return error.OutOfMemory;
        const owned_name = self.allocator.dupe(u8, name) catch {
            self.allocator.free(owned_key);
            return error.OutOfMemory;
        };
        const owned_ns = self.allocator.dupe(u8, namespace) catch {
            self.allocator.free(owned_name);
            self.allocator.free(owned_key);
            return error.OutOfMemory;
        };
        const owned_owner = self.allocator.dupe(u8, meta.owner) catch {
            self.allocator.free(owned_ns);
            self.allocator.free(owned_name);
            self.allocator.free(owned_key);
            return error.OutOfMemory;
        };
        self.actions.put(owned_key, .{
            .key_owned = owned_key,
            .name_owned = owned_name,
            .namespace_owned = owned_ns,
            .owner_owned = owned_owner,
            .action_type = meta.action_type,
            .version = version,
            .enabled = true,
            .created_at_ns = stamp_ns,
            .timeout_ms = meta.timeout_ms,
            .max_retries = meta.max_retries,
        }) catch {
            self.allocator.free(owned_owner);
            self.allocator.free(owned_ns);
            self.allocator.free(owned_name);
            self.allocator.free(owned_key);
            return error.OutOfMemory;
        };
        out.putInt(u32, version);
        out.putBytes(name);
    }

    fn freeAction(self: *ActionsHandler, rec: ActionRecord) void {
        self.allocator.free(rec.key_owned);
        self.allocator.free(rec.namespace_owned);
        self.allocator.free(rec.name_owned);
        self.allocator.free(rec.owner_owned);
    }

    /// Remove an action on replay of a delete entry.
    /// Key may be namespace-qualified (ns\x00name) or plain name (default namespace).
    fn replayDelete(self: *ActionsHandler, key: []const u8) void {
        if (self.actions.fetchRemove(key)) |old| self.freeAction(old.value);
    }

    /// Rebuild a RunRecord from a persisted invoke entry.
    /// Answered with the run's id and how many runs the shard holds.
    pub fn replayInvoke(self: *ActionsHandler, run_id: []const u8, value: []const u8, stamp_ns: u64, out: *Result) persistence.ApplyError!void {
        // An id already taken is refused, not overwritten: the run under it
        // may be running or finished, and its caller waits on that run.
        if (self.runs.contains(run_id)) {
            // A workflow's own invoke has no client to see the refusal.
            log.warn("action invoke refused: run id '{s}' is already in use", .{run_id});
            return out.refuse(.internal_error, .internal, "internal error: the server minted a run id already in use — retry");
        }
        const v = InvokeValue.parse(value) orelse return error.Halt;
        _ = try self.insertRun(run_id, v, stamp_ns);
        out.putBytes(run_id);
        out.putInt(u32, @intCast(self.runs.count()));
    }

    /// Add the run an invoke entry creates, all of it or nothing.
    fn insertRun(self: *ActionsHandler, run_id: []const u8, v: InvokeValue, stamp_ns: u64) ![]const u8 {
        const input = try self.dupeNonEmpty(v.input);
        errdefer if (input) |x| self.allocator.free(x);
        const labels = try self.dupeNonEmpty(v.labels);
        errdefer if (labels) |x| self.allocator.free(x);
        const caller_run_id = try self.dupeNonEmpty(v.caller_run_id);
        errdefer if (caller_run_id) |x| self.allocator.free(x);
        const caller_workflow_name = try self.dupeNonEmpty(v.caller_workflow_name);
        errdefer if (caller_workflow_name) |x| self.allocator.free(x);

        var kbuf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
        const arec = if (defKey(&kbuf, v.namespace, v.action_name)) |k| self.actions.get(k) else null;
        const max_retries: u32 = if (arec) |a| a.max_retries else 3;
        const owned_run_id = try self.allocator.dupe(u8, run_id);
        errdefer self.allocator.free(owned_run_id);
        const owned_action_name = try self.allocator.dupe(u8, v.action_name);
        errdefer self.allocator.free(owned_action_name);
        const owned_ns = try self.allocator.dupe(u8, v.namespace);
        errdefer self.allocator.free(owned_ns);

        try self.runs.put(owned_run_id, .{
            .run_id_owned = owned_run_id,
            .action_name_owned = owned_action_name,
            .namespace_owned = owned_ns,
            .input_owned = input,
            .labels_owned = labels,
            .caller_run_id_owned = caller_run_id,
            .caller_workflow_name_owned = caller_workflow_name,
            .source = if (caller_run_id != null) @as(u8, 1) else @as(u8, 0),
            .status = v.status,
            .created_at_ms = @intCast(stamp_ns / std.time.ns_per_ms),
            .started_at_ms = null,
            .completed_at_ms = null,
            .max_retries = max_retries,
        });
        return owned_run_id;
    }

    /// An owned copy of `bytes`, or null when it is empty.
    fn dupeNonEmpty(self: *ActionsHandler, bytes: []const u8) !?[]const u8 {
        if (bytes.len == 0) return null;
        return try self.allocator.dupe(u8, bytes);
    }

    /// The namespace an invoke value names (its last field), "default" if
    /// none.
    fn invokeNamespace(value: []const u8) []const u8 {
        return if (InvokeValue.parse(value)) |v| v.namespace else "default";
    }

    /// An invoke entry's value (`encodeInvokeValue`), read in one place:
    /// the applier and the cross-shard start both read it from here. Slices
    /// borrow from the value; an empty field reads as empty. Null when the
    /// value is cut short or its status unknown.
    const InvokeValue = struct {
        action_name: []const u8,
        status: ActionRunStatus,
        input: []const u8,
        labels: []const u8,
        caller_run_id: []const u8,
        caller_workflow_name: []const u8,
        namespace: []const u8,

        fn parse(value: []const u8) ?InvokeValue {
            var off: usize = 0;
            const action_name = readLenPrefixed(value, &off, u16) orelse return null;
            if (off >= value.len) return null;
            const status = std.enums.fromInt(ActionRunStatus, value[off]) orelse return null;
            off += 1;
            const input = readLenPrefixed(value, &off, u32) orelse return null;
            const labels = readLenPrefixed(value, &off, u32) orelse return null;
            const caller_run_id = readLenPrefixed(value, &off, u16) orelse return null;
            const caller_workflow_name = readLenPrefixed(value, &off, u16) orelse return null;
            const ns = readLenPrefixed(value, &off, u16) orelse return null;
            return .{
                .action_name = action_name,
                .status = status,
                .input = input,
                .labels = labels,
                .caller_run_id = caller_run_id,
                .caller_workflow_name = caller_workflow_name,
                .namespace = if (ns.len == 0) "default" else ns,
            };
        }
    };

    fn readLenPrefixed(value: []const u8, off: *usize, comptime L: type) ?[]const u8 {
        if (off.* + @sizeOf(L) > value.len) return null;
        const n = std.mem.readInt(L, value[off.*..][0..@sizeOf(L)], .little);
        off.* += @sizeOf(L);
        if (off.* + n > value.len) return null;
        defer off.* += n;
        return value[off.*..][0..n];
    }

    fn freeRun(self: *ActionsHandler, run: RunRecord) void {
        self.allocator.free(run.run_id_owned);
        self.allocator.free(run.action_name_owned);
        self.allocator.free(run.namespace_owned);
        if (run.input_owned) |v| self.allocator.free(v);
        if (run.labels_owned) |v| self.allocator.free(v);
        if (run.result_owned) |v| self.allocator.free(v);
        if (run.outcome_owned) |v| self.allocator.free(v);
        if (run.worker_id_owned) |v| self.allocator.free(v);
        if (run.error_owned) |v| self.allocator.free(v);
        if (run.caller_run_id_owned) |v| self.allocator.free(v);
        if (run.caller_workflow_name_owned) |v| self.allocator.free(v);
    }

    /// Apply a run status update from its entry.
    fn replayUpdateRun(self: *ActionsHandler, run_id: []const u8, value: []const u8, stamp_ns: u64) persistence.ApplyError!void {
        var off: usize = 0;

        // status
        if (off >= value.len) return error.Halt;
        const status = std.enums.fromInt(ActionRunStatus, value[off]) orelse return error.Halt;
        off += 1;

        // started_at_ms
        if (off >= value.len) return error.Halt;
        const has_started = value[off] == 1;
        off += 1;
        var started_at_ms: ?i64 = null;
        if (has_started) {
            if (off + 8 > value.len) return error.Halt;
            started_at_ms = std.mem.readInt(i64, value[off..][0..8], .little);
            off += 8;
        }

        // result data
        if (off + 4 > value.len) return error.Halt;
        const rlen: usize = std.mem.readInt(u32, value[off..][0..4], .little);
        off += 4;
        var fields: RunUpdateFields = .{};
        if (rlen > 0 and off + rlen <= value.len) {
            fields.result = value[off .. off + rlen];
            off += rlen;
        }

        // worker_id
        if (off + 2 <= value.len) {
            const wid_len: usize = std.mem.readInt(u16, value[off..][0..2], .little);
            off += 2;
            if (wid_len > 0 and off + wid_len <= value.len) fields.worker_id = value[off .. off + wid_len];
            off += wid_len;
        }

        // outcome, error
        if (off + 2 <= value.len) {
            const olen: usize = std.mem.readInt(u16, value[off..][0..2], .little);
            off += 2;
            if (olen > 0 and off + olen <= value.len) fields.outcome = value[off .. off + olen];
            off += olen;
        }
        if (off + 2 <= value.len) {
            const elen: usize = std.mem.readInt(u16, value[off..][0..2], .little);
            off += 2;
            if (elen > 0 and off + elen <= value.len) fields.err = value[off .. off + elen];
            off += elen;
        }

        const run = self.runs.getPtr(run_id) orelse {
            // An update for a run this shard never created: the log
            // orders invoke first, so the update has nothing to change.
            log.err("action run update for unknown run '{s}': no run to change", .{run_id});
            return error.Halt;
        };
        // A finished run stays finished: two reports can both pass the
        // leader's check before either applies.
        if (finished(run.status)) return;
        self.updateRun(run, status, started_at_ms, @intCast(stamp_ns / std.time.ns_per_ms), fields) catch return error.OutOfMemory;
    }

    /// The optional fields of a run update, borrowed from its entry.
    const RunUpdateFields = struct {
        result: ?[]const u8 = null,
        worker_id: ?[]const u8 = null,
        outcome: ?[]const u8 = null,
        err: ?[]const u8 = null,
    };

    /// Apply a run update, all of it or none: every copy is made before the
    /// run changes.
    fn updateRun(self: *ActionsHandler, run: *RunRecord, status: ActionRunStatus, started_at_ms: ?i64, stamp_ms: i64, fields: RunUpdateFields) !void {
        const result_data = if (fields.result) |x| try self.allocator.dupe(u8, x) else null;
        errdefer if (result_data) |x| self.allocator.free(x);
        const worker_id_data = if (fields.worker_id) |x| try self.allocator.dupe(u8, x) else null;
        errdefer if (worker_id_data) |x| self.allocator.free(x);
        const outcome_data = if (fields.outcome) |x| try self.allocator.dupe(u8, x) else null;
        errdefer if (outcome_data) |x| self.allocator.free(x);
        const error_data = if (fields.err) |x| try self.allocator.dupe(u8, x) else null;

        run.status = status;
        run.expiry_proposed = false;
        if (finished(status)) {
            run.completed_at_ms = stamp_ms;
        } else if (status == .pending) {
            run.started_at_ms = null;
        }
        if (started_at_ms) |sa| {
            run.started_at_ms = sa;
        }
        if (worker_id_data) |wid| {
            if (run.worker_id_owned) |old| self.allocator.free(old);
            run.worker_id_owned = wid;
        }
        if (result_data) |rd| {
            if (run.result_owned) |old| self.allocator.free(old);
            run.result_owned = rd;
        }
        if (outcome_data) |od| {
            if (run.outcome_owned) |old| self.allocator.free(old);
            run.outcome_owned = od;
        }
        if (error_data) |ed| {
            if (run.error_owned) |old| self.allocator.free(old);
            run.error_owned = ed;
        } else if (status == .completed) {
            // A completion clears the error of an earlier failed attempt.
            if (run.error_owned) |old| self.allocator.free(old);
            run.error_owned = null;
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Waiter Callbacks
// ═══════════════════════════════════════════════════════════════════════════════

const Waiter = waiter_pool_mod.Waiter;

/// The wall clock, as the stamp of an apply with no shard (tests), where
/// there is no log to stamp it.
fn wallNs() u64 {
    return @intCast(@max(0, @import("stdx").time.nanoTimestamp()));
}

/// Action await resolver: claim a pending run matching any action the worker handles.
/// Waiter key is compound: [namespace\x00][action_name][worker_id]
/// min_version packs: (ns_len << 16) | action_name_len.
/// Uses hash-based routing to check only the shard that owns each action.
fn resolveActionAwait(waiter: *Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const parts = AwaitKey.parse(waiter.key(), waiter.min_version) orelse return false;
    const namespace = parts.namespace;
    const action_name = parts.action_name;
    const worker_id = parts.worker_id;

    // The worker's labels and actions: the await was routed to the shard
    // its registration lives on.
    var worker_labels: ?[]const u8 = null;
    var worker_ptr: ?*const WorkerRecord = null;
    if (worker_id.len > 0) {
        // A worker drained, or gone silent, while it waited takes no more work.
        if (shard.worker_handler.refusesWork(namespace, worker_id)) |refusal| {
            shard.deliverDeferredRefusal(waiter.reply_to, waiter.request_id, .bad_request, refusal);
            return true;
        }
        if (shard.worker_handler.find(namespace, worker_id)) |w| {
            worker_labels = w.metadata_owned;
            worker_ptr = w;
        }
    }

    // Try to claim a pending run for the primary action name — hash-routed O(1)
    if (claimFromTargetShard(shard, namespace, action_name, worker_labels, worker_id)) |task| {
        deliverTaskAssignment(shard, waiter.reply_to, waiter.request_id, task);
        return true;
    }

    // Try all the worker's other registered action processes.
    if (worker_ptr) |worker| {
        for (worker.processes.items) |process| {
            if (process.kind != .action) continue;
            // Skip the primary name we already tried
            if (std.mem.eql(u8, process.name_owned, action_name)) continue;
            if (claimFromTargetShard(shard, namespace, process.name_owned, worker_labels, worker_id)) |task| {
                deliverTaskAssignment(shard, waiter.reply_to, waiter.request_id, task);
                return true;
            }
        }
    }

    return false;
}

/// A parked await's waiter key, `[namespace\x00][action_name][worker_id]`,
/// whole: a cut worker id would find no registration, and with it lose the
/// worker's labels and other actions. The waiter's min_version packs the
/// lengths, `(ns_len << 16) | action_name_len`.
const AwaitKey = struct {
    bytes: []const u8,
    lengths: u64,

    fn build(buf: []u8, namespace: []const u8, action_name: []const u8, worker_id: []const u8) ?AwaitKey {
        if (action_name.len > std.math.maxInt(u16) or namespace.len > std.math.maxInt(u16)) return null;
        const bytes = std.fmt.bufPrint(buf, "{s}\x00{s}{s}", .{ namespace, action_name, worker_id }) catch return null;
        return .{ .bytes = bytes, .lengths = (@as(u64, namespace.len) << 16) | action_name.len };
    }

    const Parts = struct { namespace: []const u8, action_name: []const u8, worker_id: []const u8 };

    fn parse(key: []const u8, lengths: u64) ?Parts {
        const ns_len: usize = @intCast(lengths >> 16);
        const action_name_len: usize = @intCast(lengths & 0xFFFF);
        if (ns_len + 1 + action_name_len > key.len) return null;
        return .{
            .namespace = key[0..ns_len],
            .action_name = key[ns_len + 1 ..][0..action_name_len],
            .worker_id = key[ns_len + 1 + action_name_len ..],
        };
    }
};

/// Claim a pending run from the shard that owns the action (via hash routing).
/// O(1) — no peer scan needed since action_invoke is pre-routed by action name.
fn claimFromTargetShard(shard: *Shard, namespace: []const u8, action_name: []const u8, worker_labels: ?[]const u8, worker_id: []const u8) ?ActionsHandler.ClaimedTask {
    const target = ActionsHandler.resolveActionShard(shard, namespace, action_name);
    return target.actions_handler.claimPendingRun(shard.allocator, namespace, action_name, worker_labels, worker_id);
}

/// A task assignment's body:
///   [task_id_len:u16][task_id][task_type_len:u16][task_type][created_at:i64][attempt:u32]
///   [has_caller:u8]([caller_run_id_len:u16][caller_run_id][caller_wf_len:u16][caller_wf])?[payload]
/// Sized exactly and allocated, so an assignment is sent whole or not at all.
fn encodeTaskAssignment(allocator: std.mem.Allocator, run: *const ActionsHandler.RunRecord, attempt: u32) ![]u8 {
    comptime std.debug.assert(MAX_INVOKE_VALUE + run_id_mod.MAX_ID_LEN + 64 <= shard_mod.MAX_REQUEST_SIZE);
    const task = .{ .run_id = run.run_id_owned, .action_name = run.action_name_owned, .created_at_ms = run.created_at_ms, .attempt = attempt };
    const payload = run.input_owned orelse "";
    const caller_run_id = run.caller_run_id_owned orelse "";
    const caller_wf_name = run.caller_workflow_name_owned orelse "";
    const has_caller: u8 = if (run.caller_run_id_owned != null) 1 else 0;
    const caller_extra: usize = if (has_caller == 1) (2 + caller_run_id.len + 2 + caller_wf_name.len) else 0;
    const buf = try allocator.alloc(u8, 2 + task.run_id.len + 2 + task.action_name.len + 8 + 4 + 1 + caller_extra + payload.len);
    var pos: usize = 0;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(task.run_id.len), .little);
    pos += 2;
    @memcpy(buf[pos .. pos + task.run_id.len], task.run_id);
    pos += task.run_id.len;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(task.action_name.len), .little);
    pos += 2;
    @memcpy(buf[pos .. pos + task.action_name.len], task.action_name);
    pos += task.action_name.len;
    std.mem.writeInt(i64, buf[pos..][0..8], task.created_at_ms, .little);
    pos += 8;
    // attempt (tracked across retries)
    std.mem.writeInt(u32, buf[pos..][0..4], task.attempt, .little);
    pos += 4;
    buf[pos] = has_caller;
    pos += 1;
    if (has_caller == 1) {
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(caller_run_id.len), .little);
        pos += 2;
        @memcpy(buf[pos .. pos + caller_run_id.len], caller_run_id);
        pos += caller_run_id.len;
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(caller_wf_name.len), .little);
        pos += 2;
        @memcpy(buf[pos .. pos + caller_wf_name.len], caller_wf_name);
        pos += caller_wf_name.len;
    }
    @memcpy(buf[pos..], payload);
    std.debug.assert(buf.len <= shard_mod.MAX_REQUEST_SIZE);
    return buf;
}

/// Answer the await being dispatched with its task. It goes through the
/// connection like any answer: delivering it as deferred as well would let
/// the dispatcher, seeing no answer, send a second one under the same id.
fn answerTaskAssignment(shard: *Shard, conn: *Connection, request_id: u64, task: ActionsHandler.ClaimedTask) void {
    defer shard.allocator.free(task.body);
    shard.sendOkResponse(conn, request_id, task.body);
}

/// Deliver a task to a parked await, whose request was answered as deferred.
fn deliverTaskAssignment(shard: *Shard, reply_to: ReplyTo, request_id: u64, task: ActionsHandler.ClaimedTask) void {
    defer shard.allocator.free(task.body);
    shard.deliverDeferredOk(reply_to, request_id, task.body);
}

/// Largest invoke entry value; every run, and so every task, is built from
/// one. An invoke is a log entry, so it can be no larger than an entry's
/// payload, whichever path proposed it.
const MAX_INVOKE_VALUE = persistence.MAX_PERSIST_PAYLOAD;

/// Extract the first task type (action name) from the action_await value.
/// Wire format: [count:u32][type_len:u16][type_name]...
fn extractFirstTaskType(value: []const u8) ?[]const u8 {
    if (value.len < 6) return null; // need at least count(4) + len(2)
    const count = std.mem.readInt(u32, value[0..4], .little);
    if (count == 0) return null;
    const type_len: usize = std.mem.readInt(u16, value[4..6], .little);
    if (value.len < 6 + type_len) return null;
    const name = value[6 .. 6 + type_len];
    if (name.len == 0) return null;
    return name;
}

/// Iterator over task types in the action_await wire format.
/// Wire format: [count:u32][type_len:u16][type_name]...
const TaskTypeIterator = struct {
    data: []const u8,
    offset: usize,
    remaining: u32,

    fn init(value: []const u8) TaskTypeIterator {
        if (value.len < 4) return .{ .data = value, .offset = 0, .remaining = 0 };
        const count = std.mem.readInt(u32, value[0..4], .little);
        return .{ .data = value, .offset = 4, .remaining = count };
    }

    /// The next non-empty type name. A loop, not recursion: a request can
    /// carry tens of thousands of empty names.
    fn next(self: *TaskTypeIterator) ?[]const u8 {
        while (self.remaining > 0) {
            if (self.offset + 2 > self.data.len) return null;
            const type_len: usize = std.mem.readInt(u16, self.data[self.offset..][0..2], .little);
            self.offset += 2;
            if (self.offset + type_len > self.data.len) return null;
            const name = self.data[self.offset .. self.offset + type_len];
            self.offset += type_len;
            self.remaining -= 1;
            if (name.len > 0) return name;
        }
        return null;
    }
};

/// An invoke request value: `[has_labels:u8]([labels_len:u16][labels])?[input...]`.
/// Labels are refused unless they are a JSON object: anything else could
/// never match a worker, and the run would wait forever.
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

/// Check if worker_labels satisfy all required_labels.
/// Both are JSON object strings e.g. {"gpu":true,"region":"us-east"}.
/// Returns true if every key-value in required exists with the same value in worker.
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

    const req_keys = req_obj.keys();
    const req_values = req_obj.values();
    for (req_keys, req_values) |rkey, rval| {
        const wval = wrk_obj.get(rkey) orelse return false;
        if (!jsonValEq(rval, wval)) return false;
    }
    return true;
}

/// Compare two JSON scalar values for equality.
fn jsonValEq(a: std.json.Value, b: std.json.Value) bool {
    return switch (a) {
        .null => switch (b) {
            .null => true,
            else => false,
        },
        .bool => |va| switch (b) {
            .bool => |vb| va == vb,
            else => false,
        },
        .integer => |va| switch (b) {
            .integer => |vb| va == vb,
            else => false,
        },
        .float => |va| switch (b) {
            .float => |vb| va == vb,
            else => false,
        },
        .string => |va| switch (b) {
            .string => |vb| std.mem.eql(u8, va, vb),
            else => false,
        },
        .number_string => |va| switch (b) {
            .number_string => |vb| std.mem.eql(u8, va, vb),
            else => false,
        },
        .array, .object => false,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Response Serialization — CommandResult → Wire Response
// ═══════════════════════════════════════════════════════════════════════════════

/// Convert a CommandResult to a wire response and queue it on the connection.
fn sendActionResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .ok, .action_deleted => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .err => |e| {
            shard.sendRefusal(conn, request_id, e.code.toStatus(), e.refusal());
        },
        .action_registered => |r| {
            shard.sendOkResponse(conn, request_id, r.name);
        },
        .action_invoked => |i| {
            // Wire format: [run_id_len:u16][run_id][has_output:u8]
            var buf: [4096]u8 = undefined;
            var fbs: std.Io.Writer = .fixed(&buf);
            const writer = &fbs;
            writer.writeInt(u16, @intCast(i.run_id.len), .little) catch return;
            writer.writeAll(i.run_id) catch return;
            writer.writeByte(0) catch return; // has_output (always false)
            shard.sendOkResponse(conn, request_id, fbs.buffered());
        },
        .action_list_result => |l| {
            shard.sendOkResponse(conn, request_id, l.data);
        },
        else => {
            shard.sendErrorResponse(conn, request_id, .internal_error, "unhandled action response");
        },
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn makeRequest(op: OpCode, key: []const u8, value: []const u8) Request {
    return .{
        .header = proto.RequestHeader.init(op, 1),
        .namespace = "default",
        .key = key,
        .value = value,
        .options = "",
    };
}

fn applyRegister(h: *ActionsHandler, key: []const u8, value: []const u8, stamp_ns: u64) !void {
    var out: Result = .{};
    return h.replayRegister(key, value, stamp_ns, &out);
}

fn applyInvoke(h: *ActionsHandler, run_id: []const u8, value: []const u8, stamp_ns: u64) !void {
    var out: Result = .{};
    return h.replayInvoke(run_id, value, stamp_ns, &out);
}

fn applyLogged(h: *ActionsHandler, e: *const Entry) !void {
    var out: Result = .{};
    return h.replayEntry(e, &out);
}

test "actions handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    ActionsHandler.register(&dispatcher);

    try testing.expect(dispatcher.hasHandler(OpCode.action_register));
    try testing.expect(dispatcher.hasHandler(OpCode.action_invoke));
    try testing.expect(dispatcher.hasHandler(OpCode.action_status));
    try testing.expect(dispatcher.hasHandler(OpCode.action_list));
    try testing.expect(dispatcher.hasHandler(OpCode.action_delete));
    try testing.expect(dispatcher.hasHandler(OpCode.action_await));
    try testing.expect(dispatcher.hasHandler(OpCode.action_complete));
    try testing.expect(dispatcher.hasHandler(OpCode.action_fail));
    try testing.expect(dispatcher.hasHandler(OpCode.action_touch));
    try testing.expect(dispatcher.hasHandler(OpCode.action_list_runs));

    try testing.expectEqual(@as(u16, 10), dispatcher.handler_count);
}

test "actions handler: register" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(null, makeRequest(.action_register, "my-action", ""));
    switch (result) {
        .action_registered => |r| {
            try testing.expectEqualStrings("my-action", r.name);
            try testing.expectEqualStrings("1", r.version);
        },
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 1), handler.actionCount());
}

test "actions handler: register re-register bumps version" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(null, makeRequest(.action_register, "my-action", ""));
    const result = handler.handleCommand(null, makeRequest(.action_register, "my-action", ""));
    switch (result) {
        .action_registered => |r| {
            try testing.expectEqualStrings("2", r.version);
        },
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 1), handler.actionCount());
}

test "actions handler: register empty name" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(null, makeRequest(.action_register, "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: an invoke value is a has_labels byte, the labels if set, then the input" {
    const a = testing.allocator;

    const plain = try parseInvokeValue(a, "\x00x");
    try testing.expectEqual(@as(?[]const u8, null), plain.labels);
    try testing.expectEqualStrings("x", plain.input);

    const empty_input = try parseInvokeValue(a, "\x00");
    try testing.expectEqualStrings("", empty_input.input);

    const labelled = try parseInvokeValue(a, "\x01\x0c\x00{\"gpu\":true}x");
    try testing.expectEqualStrings("{\"gpu\":true}", labelled.labels.?);
    try testing.expectEqualStrings("x", labelled.input);

    try testing.expectError(error.Empty, parseInvokeValue(a, ""));
    try testing.expectError(error.BadFlag, parseInvokeValue(a, "\x02x"));
    try testing.expectError(error.Truncated, parseInvokeValue(a, "\x01\x0c"));
    try testing.expectError(error.Truncated, parseInvokeValue(a, "\x01\x0d\x00{\"gpu\":true}"));
    try testing.expectError(error.LabelsNotObject, parseInvokeValue(a, "\x01\x04\x00truex"));
    try testing.expectError(error.LabelsNotObject, parseInvokeValue(a, "\x01\x03\x00{\"gx"));
}

test "actions handler: a malformed invoke value is refused before a run is created" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(null, makeRequest(.action_register, "process", ""));
    for ([_][]const u8{ "", "\x02x", "\x01\x05\x00ab", "\x01\x02\x00[]" }) |value| {
        switch (handler.handleCommand(null, makeRequest(.action_invoke, "process", value))) {
            .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
            else => return error.TestUnexpectedResult,
        }
    }
    try testing.expectEqual(@as(usize, 0), handler.runCount());
}

test "actions handler: invoke" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(null, makeRequest(.action_register, "process", ""));

    const result = handler.handleCommand(null, makeRequest(.action_invoke, "process", "\x00input-data"));
    switch (result) {
        .action_invoked => |r| {
            try testing.expect(r.run_id.len > 0);
            try testing.expect(r.queue_position != null);
        },
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 1), handler.runCount());
}

test "actions handler: a register answers with its name whole, at the longest key an entry carries" {
    var handler = ActionsHandler.init(testing.allocator);
    defer handler.deinit();
    var key: [persistence.MAX_ENTRY_KEY]u8 = @splat('a');
    key[3] = ns_keys.NAMESPACE_SEPARATOR;
    var out: Result = .{};
    try handler.replayRegister(&key, "", 1, &out);
    try testing.expect(!out.overflowed);
    var ver: [12]u8 = undefined;
    const answer = ActionsHandler.registeredFrom(&out, &ver).action_registered;
    try testing.expectEqualSlices(u8, key[4..], answer.name);
    try testing.expectEqualStrings("1", answer.version);
}

test "actions handler: an invoke naming a run id already taken is refused, and the run it names is kept" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();
    _ = handler.handleCommand(null, makeRequest(.action_register, "process", ""));

    var buf: [256]u8 = undefined;
    const value = ActionsHandler.encodeInvokeValue(&buf, "default", "process", "first", null, null, null).?;
    var out: Result = .{};
    try handler.replayInvoke("act-1", value, 1, &out);
    try testing.expect(ActionsHandler.invokedFrom(&out) == .action_invoked);
    handler.runs.getPtr("act-1").?.status = .running;

    const again = ActionsHandler.encodeInvokeValue(&buf, "default", "process", "second", null, null, null).?;
    var refused: Result = .{};
    try handler.replayInvoke("act-1", again, 2, &refused);
    try testing.expectEqual(proto.StatusCode.internal_error, refused.refused().?.status);
    const kept = handler.runs.getPtr("act-1").?;
    try testing.expectEqual(ActionRunStatus.running, kept.status);
    try testing.expectEqualStrings("first", kept.input_owned.?);
    try testing.expectEqual(@as(usize, 1), handler.runCount());
}

test "actions handler: invoke non-existent action" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(null, makeRequest(.action_invoke, "ghost", "\x00data"));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.not_found, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "actions: status answers a pending run's status and times, and no output" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    var inv_buf: [512]u8 = undefined;
    try applyRegister(&h, "echo", "", 1);
    const inv = ActionsHandler.encodeInvokeValue(&inv_buf, "default", "echo", "{}", "", "", "") orelse return error.EncodeFailed;
    try applyInvoke(&h, "act-1", inv, 1000 * std.time.ns_per_ms);
    const s = try h.answerStatus("default", "act-1");
    try testing.expectEqual(action_ops.RunStatus.pending, s.status);
    try testing.expectEqual(@as(i64, 1000), s.created_at_ms);
    try testing.expectEqual(@as(?i64, null), s.started_at_ms);
    try testing.expectEqual(@as(u32, 0), s.attempt);
    try testing.expectEqual(@as(?[]const u8, null), s.output);
    try testing.expectEqual(@as(?[]const u8, null), s.error_message);
    // No worker has taken it.
    try testing.expectEqual(@as(?[]const u8, null), s.worker_id);
}

test "actions: status of a completed run carries the output its worker completed it with" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testRunning(&h, "w-1");
    var buf: [128]u8 = undefined;
    try testing.expect(h.handleActionComplete(null, testReport(&buf, .action_complete, "w-1", "act-1", false)) == .applied);
    const s = try h.answerStatus("default", "act-1");
    try testing.expectEqual(action_ops.RunStatus.completed, s.status);
    try testing.expectEqualStrings("{}", s.output.?);
    try testing.expectEqual(@as(u32, 1), s.attempt);
    try testing.expectEqualStrings("w-1", s.worker_id.?);
    try testing.expect(s.completed_at_ms != null);
}

test "actions: status of a failed run carries its error and no output" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testRunning(&h, "w-1");
    // [action_name][run id][retry=0][error...]
    const error_len = 1000;
    const buf = try std.testing.allocator.alloc(u8, 2 + 4 + 2 + 5 + 1 + error_len);
    defer std.testing.allocator.free(buf);
    var off: usize = 0;
    for ([_][]const u8{ "echo", "act-1" }) |f| {
        std.mem.writeInt(u16, buf[off..][0..2], @intCast(f.len), .little);
        @memcpy(buf[off + 2 ..][0..f.len], f);
        off += 2 + f.len;
    }
    buf[off] = 0;
    @memset(buf[off + 1 ..], 'e');
    const req: Request = .{ .header = proto.RequestHeader.init(.action_fail, 1), .namespace = "default", .key = "w-1", .value = buf, .options = "" };
    try testing.expect(h.handleActionFail(null, req) == .applied);
    const s = try h.answerStatus("default", "act-1");
    try testing.expectEqual(action_ops.RunStatus.failed, s.status);
    try testing.expectEqual(@as(?[]const u8, null), s.output);
    try testing.expectEqual(@as(usize, error_len), s.error_message.?.len);
    try testing.expect(std.mem.allEqual(u8, s.error_message.?, 'e'));
}

test "actions: status refuses an output or error longer than its field rather than cutting it" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testRunning(&h, "w-1");
    var buf: [128]u8 = undefined;
    try testing.expect(h.handleActionComplete(null, testReport(&buf, .action_complete, "w-1", "act-1", false)) == .applied);
    // No complete or fail stores this much today, so store it directly.
    const run = h.runs.getPtr("act-1").?;
    if (run.result_owned) |r| h.allocator.free(r);
    run.result_owned = try h.allocator.alloc(u8, action_ops.MAX_OUTPUT + 1);
    @memset(@constCast(run.result_owned.?), 'o');
    try testing.expectError(error.StoredTooLong, h.answerStatus("default", "act-1"));
    h.allocator.free(run.result_owned.?);
    run.result_owned = try h.allocator.dupe(u8, "ok");
    if (run.error_owned) |e| h.allocator.free(e);
    run.error_owned = try h.allocator.alloc(u8, action_ops.MAX_ERROR + 1);
    @memset(@constCast(run.error_owned.?), 'e');
    try testing.expectError(error.StoredTooLong, h.answerStatus("default", "act-1"));
}

test "actions: status of a run that doesn't exist, or is in another namespace, is not found" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testRunning(&h, "w-1");
    try testing.expectError(error.NotFound, h.answerStatus("default", "act-9999"));
    try testing.expectError(error.NotFound, h.answerStatus("elsewhere", "act-1"));
}

test "actions handler: list" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(null, makeRequest(.action_register, "alpha", ""));
    _ = handler.handleCommand(null, makeRequest(.action_register, "beta", ""));
    _ = handler.handleCommand(null, makeRequest(.action_register, "gamma", ""));

    const result = handler.handleCommand(null, makeRequest(.action_list, "", ""));
    switch (result) {
        .action_list_result => |r| {
            defer handler.freeResult(result);
            const entry_count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 3), entry_count);
            try testing.expect(r.cursor == null);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: list empty" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(null, makeRequest(.action_list, "", ""));
    switch (result) {
        .action_list_result => |r| {
            defer handler.freeResult(result);
            const entry_count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), entry_count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: delete" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(null, makeRequest(.action_register, "to-delete", ""));
    try testing.expectEqual(@as(usize, 1), handler.actionCount());

    const result = handler.handleCommand(null, makeRequest(.action_delete, "to-delete", ""));
    switch (result) {
        .action_deleted => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 0), handler.actionCount());
}

test "actions handler: delete non-existent is idempotent" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(null, makeRequest(.action_delete, "ghost", ""));
    switch (result) {
        .action_deleted => {},
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: pre-route by action" {
    const req1 = makeRequest(.action_invoke, "action-a", "\x00");
    const req2 = makeRequest(.action_invoke, "action-a", "\x00");
    const req3 = makeRequest(.action_invoke, "action-b", "\x00");

    try testing.expectEqual(ActionsHandler.preRouteByAction(req1), ActionsHandler.preRouteByAction(req2));
    try testing.expect(ActionsHandler.preRouteByAction(req1) != ActionsHandler.preRouteByAction(req3));

    const req_empty = makeRequest(.action_invoke, "", "\x00");
    try testing.expectEqual(@as(?u64, 0), ActionsHandler.preRouteByAction(req_empty));

    // Same action, different namespace → different hash (namespace isolation)
    var req_ns = makeRequest(.action_invoke, "action-a", "\x00");
    req_ns.namespace = "other";
    try testing.expect(ActionsHandler.preRouteByAction(req1) != ActionsHandler.preRouteByAction(req_ns));
}

test "actions handler: multiple invocations" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(null, makeRequest(.action_register, "worker", ""));

    for (0..5) |_| {
        const result = handler.handleCommand(null, makeRequest(.action_invoke, "worker", "\x00task"));
        switch (result) {
            .action_invoked => {},
            else => return error.TestUnexpectedResult,
        }
    }

    try testing.expectEqual(@as(usize, 5), handler.runCount());
}

test "actions handler: freeResult non-allocated is no-op" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    handler.freeResult(.ok);
    handler.freeResult(.{ .err = .{ .code = .invalid_request, .message = "test" } });
    handler.freeResult(.{ .action_deleted = {} });
}

test "actions: a run update applied from its entry carries outcome, error and worker" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();

    try applyRegister(&h, "echo", "", 1);

    var inv_buf: [512]u8 = undefined;
    const inv = ActionsHandler.encodeInvokeValue(&inv_buf, "default", "echo", "{\"k\":1}", "gpu", "wf-run-9", "parent-wf") orelse return error.EncodeFailed;
    try applyInvoke(&h, "act-1", inv, 1000 * std.time.ns_per_ms);
    const created = h.runs.get("act-1") orelse return error.RunMissing;
    try std.testing.expectEqualStrings("gpu", created.labels_owned.?);
    try std.testing.expectEqualStrings("wf-run-9", created.caller_run_id_owned.?);
    try std.testing.expectEqual(@as(u8, 1), created.source);

    // A failed attempt keeps its error and worker; a completion clears the error.
    try std.testing.expect(h.applyRunUpdate(null, "default", "act-1", .{
        .status = .pending,
        .started_at_ms = 1500,
        .result = "",
        .worker_id = "w-1",
        .outcome = "",
        .error_message = "boom",
    }) == .applied);
    const failed = h.runs.get("act-1") orelse return error.RunMissing;
    try std.testing.expectEqual(ActionRunStatus.pending, failed.status);
    try std.testing.expectEqualStrings("boom", failed.error_owned.?);
    try std.testing.expectEqualStrings("w-1", failed.worker_id_owned.?);

    try std.testing.expect(h.applyRunUpdate(null, "default", "act-1", .{
        .status = .completed,
        .started_at_ms = 1500,
        .result = "{\"ok\":true}",
        .worker_id = "w-2",
        .outcome = "success",
        .error_message = "",
    }) == .applied);
    const done = h.runs.get("act-1") orelse return error.RunMissing;
    try std.testing.expectEqual(ActionRunStatus.completed, done.status);
    try std.testing.expectEqualStrings("success", done.outcome_owned.?);
    try std.testing.expectEqualStrings("{\"ok\":true}", done.result_owned.?);
    try std.testing.expectEqualStrings("w-2", done.worker_id_owned.?);
    try std.testing.expect(done.error_owned == null);
}

fn testReport(buf: []u8, op: proto.OpCode, worker: []const u8, run_id: []const u8, retry: bool) Request {
    var off: usize = 0;
    for ([_][]const u8{ "echo", run_id }) |f| {
        std.mem.writeInt(u16, buf[off..][0..2], @intCast(f.len), .little);
        @memcpy(buf[off + 2 ..][0..f.len], f);
        off += 2 + f.len;
    }
    if (op == .action_complete) {
        for ([_][]const u8{ "success", "{}" }) |f| {
            std.mem.writeInt(u16, buf[off..][0..2], @intCast(f.len), .little);
            @memcpy(buf[off + 2 ..][0..f.len], f);
            off += 2 + f.len;
        }
    } else {
        buf[off] = @intFromBool(retry);
        off += 1;
    }
    return .{ .header = proto.RequestHeader.init(op, 1), .namespace = "default", .key = worker, .value = buf[0..off], .options = "" };
}

fn testRunning(h: *ActionsHandler, worker: []const u8) !void {
    var inv_buf: [512]u8 = undefined;
    if (h.runs.get("act-1") == null) {
        try applyRegister(h, "echo", "", 1);
        const inv = ActionsHandler.encodeInvokeValue(&inv_buf, "default", "echo", "{}", "", "", "") orelse return error.EncodeFailed;
        try applyInvoke(h, "act-1", inv, 1000 * std.time.ns_per_ms);
    }
    const task = h.claimPendingRun(std.testing.allocator, "default", "echo", null, worker) orelse return error.NothingToClaim;
    std.testing.allocator.free(task.body);
}

test "actions: a report for a run that has finished is refused, and a resent one is ok" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testRunning(&h, "w-1");
    var buf: [128]u8 = undefined;

    try testing.expect(h.handleActionComplete(null, testReport(&buf, .action_complete, "w-1", "act-1", false)) == .applied);
    try testing.expectEqual(ActionRunStatus.completed, h.runs.get("act-1").?.status);
    // The same completion resent: ok, nothing new.
    try testing.expect(h.handleActionComplete(null, testReport(&buf, .action_complete, "w-1", "act-1", false)) == .repeat);
    // A late fail asking for a retry would have put it back to pending.
    const late = h.handleActionFail(null, testReport(&buf, .action_fail, "w-1", "act-1", true));
    try testing.expectEqual(proto.StatusCode.conflict, late.refused.status);
    try testing.expectEqual(proto.Reason.stale_report, late.refused.reason);
    try testing.expectEqual(ActionRunStatus.completed, h.runs.get("act-1").?.status);
}

test "actions: a resent fail can't send back an attempt another worker now runs" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testRunning(&h, "w-1");
    var buf: [128]u8 = undefined;

    try testing.expect(h.handleActionFail(null, testReport(&buf, .action_fail, "w-1", "act-1", true)) == .applied);
    try testing.expectEqual(ActionRunStatus.pending, h.runs.get("act-1").?.status);
    // Resent while it waits: the run already shows it.
    try testing.expect(h.handleActionFail(null, testReport(&buf, .action_fail, "w-1", "act-1", true)) == .repeat);

    try testRunning(&h, "w-2");
    const resent = h.handleActionFail(null, testReport(&buf, .action_fail, "w-1", "act-1", true));
    try testing.expectEqual(proto.StatusCode.conflict, resent.refused.status);
    try testing.expectEqual(proto.Reason.stale_report, resent.refused.reason);
    try testing.expectEqual(ActionRunStatus.running, h.runs.get("act-1").?.status);
    try testing.expectEqualStrings("w-2", h.runs.get("act-1").?.worker_id_owned.?);
}

test "actions: an update applied to a finished run leaves it finished" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testRunning(&h, "w-1");
    // Two reports that both passed the leader's check, applied in order.
    try testing.expect(h.applyRunUpdate(null, "default", "act-1", .{ .status = .completed, .started_at_ms = 1, .result = "{}", .worker_id = "w-1", .outcome = "success", .error_message = "" }) == .applied);
    try testing.expect(h.applyRunUpdate(null, "default", "act-1", .{ .status = .pending, .started_at_ms = null, .result = "", .worker_id = "w-1", .outcome = "", .error_message = "late" }) == .applied);
    const run = h.runs.get("act-1").?;
    try testing.expectEqual(ActionRunStatus.completed, run.status);
    try testing.expect(run.error_owned == null);
}

test "actions: an apply that runs out of memory fails and changes nothing" {
    var inv_buf: [512]u8 = undefined;
    const inv = ActionsHandler.encodeInvokeValue(&inv_buf, "default", "echo", "{\"k\":1}", "gpu", "wf-run-9", "parent-wf") orelse return error.EncodeFailed;
    var upd_buf: [256]u8 = undefined;
    const upd = ActionsHandler.encodeRunUpdateValue(&upd_buf, .{
        .status = .completed,
        .started_at_ms = 1500,
        .result = "{\"ok\":true}",
        .worker_id = "w-2",
        .outcome = "success",
        .error_message = "late",
    }) orelse return error.EncodeFailed;

    // Each allocation of each apply, failed in turn, after the applies
    // before it succeed.
    for (0..3) |stage| {
        var fail_at: usize = 0;
        while (true) : (fail_at += 1) {
            var fa = std.testing.FailingAllocator.init(std.testing.allocator, .{});
            var h = ActionsHandler.init(fa.allocator());
            defer h.deinit();

            if (stage == 0) fa.fail_index = fa.alloc_index + fail_at;
            var out: Result = .{};
            const registered = h.replayRegister("echo", "", 1, &out);
            if (stage == 0 and fa.has_induced_failure) {
                try testing.expectError(error.OutOfMemory, registered);
                try testing.expectEqual(@as(usize, 0), h.actions.count());
                continue;
            }
            try registered;
            if (stage == 0) break;

            if (stage == 1) fa.fail_index = fa.alloc_index + fail_at;
            var inv_out: Result = .{};
            const invoked = h.replayInvoke("act-1", inv, 1000 * std.time.ns_per_ms, &inv_out);
            if (stage == 1 and fa.has_induced_failure) {
                try testing.expectError(error.OutOfMemory, invoked);
                try testing.expectEqual(@as(usize, 0), h.runs.count());
                continue;
            }
            try invoked;
            if (stage == 1) break;

            fa.fail_index = fa.alloc_index + fail_at;
            const updated = h.replayUpdateRun("act-1", upd, 2000 * std.time.ns_per_ms);
            const run = h.runs.get("act-1") orelse return error.RunMissing;
            if (fa.has_induced_failure) {
                try testing.expectError(error.OutOfMemory, updated);
                // All or nothing: the run is as the invoke left it.
                try testing.expectEqual(ActionRunStatus.pending, run.status);
                try testing.expect(run.result_owned == null and run.worker_id_owned == null and run.outcome_owned == null and run.error_owned == null);
                continue;
            }
            try updated;
            try testing.expectEqualStrings("late", run.error_owned.?);
            break;
        }
    }
}

test "actions: an action's created, run's created and run's finished times are their entries' stamps, not the host's clock" {
    const host_ms = @import("stdx").time.milliTimestamp();
    // One stamp far behind the host's clock and one a day ahead: an applier
    // reading the clock misses one or the other.
    for ([_]i64{ 1_000, host_ms + std.time.ms_per_day }) |stamp_ms| {
        var h = ActionsHandler.init(std.testing.allocator);
        defer h.deinit();
        const stamp_ns: u64 = @as(u64, @intCast(stamp_ms)) * std.time.ns_per_ms;
        var pbuf: [512]u8 = undefined;
        // Through `replayEntry`, as the log applies them.
        try applyLogged(&h, &entry_mod.buildCommandEntry(.action_register, 0, 1, 1, stamp_ns, 0, "echo", "", &pbuf).?);
        try std.testing.expectEqual(stamp_ns, h.actions.get("echo").?.created_at_ns);

        var inv_buf: [512]u8 = undefined;
        // A namespace other than default: its field comes last, after every
        // field the layout has.
        const inv = ActionsHandler.encodeInvokeValue(&inv_buf, "ns1", "echo", "", null, null, null).?;
        try applyLogged(&h, &entry_mod.buildCommandEntry(.action_invoke, 0, 1, 2, stamp_ns + 5 * std.time.ns_per_ms, 0, "act-1", inv, &pbuf).?);
        try std.testing.expectEqual(stamp_ms + 5, h.runs.get("act-1").?.created_at_ms);
        try std.testing.expectEqualStrings("ns1", h.runs.get("act-1").?.namespace_owned);

        var upd_buf: [256]u8 = undefined;
        const upd = ActionsHandler.encodeRunUpdateValue(&upd_buf, .{ .status = .completed, .started_at_ms = 7, .result = "", .worker_id = null, .outcome = "", .error_message = "" }).?;
        try applyLogged(&h, &entry_mod.buildCommandEntry(.action_update_run, 0, 1, 3, stamp_ns + 9 * std.time.ns_per_ms, 0, "act-1", upd, &pbuf).?);
        const done = h.runs.get("act-1").?;
        try std.testing.expectEqual(@as(?i64, stamp_ms + 9), done.completed_at_ms);
        // The claim's time is carried as the claiming node took it.
        try std.testing.expectEqual(@as(?i64, 7), done.started_at_ms);
    }
}

test "actions: an invoke value reads back every field it was written with, and a value cut short reads as nothing" {
    var buf: [256]u8 = undefined;
    const value = ActionsHandler.encodeInvokeValue(&buf, "ns1", "echo", "in", "gpu", "wf-run-9", "parent-wf").?;
    const v = ActionsHandler.InvokeValue.parse(value).?;
    try std.testing.expectEqualStrings("echo", v.action_name);
    try std.testing.expectEqual(ActionRunStatus.pending, v.status);
    try std.testing.expectEqualStrings("in", v.input);
    try std.testing.expectEqualStrings("gpu", v.labels);
    try std.testing.expectEqualStrings("wf-run-9", v.caller_run_id);
    try std.testing.expectEqualStrings("parent-wf", v.caller_workflow_name);
    try std.testing.expectEqualStrings("ns1", v.namespace);
    try std.testing.expectEqualStrings("ns1", ActionsHandler.invokeNamespace(value));
    for (0..value.len) |n| try std.testing.expect(ActionsHandler.InvokeValue.parse(value[0..n]) == null);
}

test "actions: an action name holding a NUL has no registry key" {
    var buf: [ns_keys.MAX_QUALIFIED_KEY]u8 = undefined;
    try std.testing.expectEqualStrings("x", ActionsHandler.defKey(&buf, "default", "x").?);
    try std.testing.expect(ActionsHandler.defKey(&buf, "", "b\x00x") == null);
}

test "actions: a claimed task owns its bytes, so the run can be freed while it is sent" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try applyRegister(&h, "echo", "", 1);
    var inv_buf: [512]u8 = undefined;
    const inv = ActionsHandler.encodeInvokeValue(&inv_buf, "default", "echo", "payload", "", "", "") orelse return error.EncodeFailed;
    try applyInvoke(&h, "act-1", inv, 1000 * std.time.ns_per_ms);

    const task = h.claimPendingRun(std.testing.allocator, "default", "echo", null, "w-1") orelse return error.NothingToClaim;
    defer std.testing.allocator.free(task.body);
    // The run's shard frees it (a delete applying) before the task is sent.
    const gone = h.runs.fetchRemove("act-1").?;
    h.freeRun(gone.value);

    try testing.expectEqualStrings("act-1", task.run_id);
    try testing.expect(std.mem.endsWith(u8, task.body, "payload"));
}

test "actions: a claim with no memory for its task leaves the run as it was" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try applyRegister(&h, "echo", "", 1);
    var inv_buf: [512]u8 = undefined;
    const inv = ActionsHandler.encodeInvokeValue(&inv_buf, "default", "echo", "payload", "", "", "") orelse return error.EncodeFailed;
    try applyInvoke(&h, "act-1", inv, 1000 * std.time.ns_per_ms);

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try testing.expect(h.claimPendingRun(failing.allocator(), "default", "echo", null, "w-1") == null);
    const run = h.runs.get("act-1").?;
    try testing.expectEqual(ActionRunStatus.pending, run.status);
    try testing.expect(run.worker_id_owned == null);
    try testing.expectEqual(@as(u32, 0), run.attempt);

    const task = h.claimPendingRun(std.testing.allocator, "default", "echo", null, "w-1") orelse return error.NothingToClaim;
    defer std.testing.allocator.free(task.body);
    try testing.expectEqual(@as(u32, 1), h.runs.get("act-1").?.attempt);
}

test "encodeTaskAssignment: a large input is encoded whole, caller block included" {
    // Past the old 8 KiB buffer, which sent the run id alone as the body.
    const input = try std.testing.allocator.alloc(u8, 65_536);
    defer std.testing.allocator.free(input);
    for (input, 0..) |*b, i| b.* = @truncate(i);
    const run: ActionsHandler.RunRecord = .{
        .run_id_owned = "run-1",
        .action_name_owned = "act",
        .namespace_owned = "default",
        .input_owned = input,
        .caller_run_id_owned = "wf-run",
        .caller_workflow_name_owned = "wf",
        .status = .pending,
        .created_at_ms = 7,
        .started_at_ms = null,
        .completed_at_ms = null,
    };
    const body = try encodeTaskAssignment(std.testing.allocator, &run, 2);
    defer std.testing.allocator.free(body);

    var pos: usize = 0;
    try std.testing.expectEqualStrings("run-1", body[2..][0..std.mem.readInt(u16, body[0..2], .little)]);
    pos = 2 + 5;
    try std.testing.expectEqualStrings("act", body[pos + 2 ..][0..std.mem.readInt(u16, body[pos..][0..2], .little)]);
    pos += 2 + 3;
    try std.testing.expectEqual(@as(i64, 7), std.mem.readInt(i64, body[pos..][0..8], .little));
    pos += 8;
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, body[pos..][0..4], .little));
    pos += 4;
    try std.testing.expectEqual(@as(u8, 1), body[pos]);
    pos += 1 + 2 + "wf-run".len + 2 + "wf".len;
    try std.testing.expectEqualSlices(u8, input, body[pos..]);
}

/// Register "echo" with `timeout_ms` and `max_retries`, invoke run act-1, and
/// claim it for w-1 at a known time.
fn testTimedRun(h: *ActionsHandler, timeout_ms: u32, max_retries: u32, started_at_ms: i64) !void {
    var reg: [9]u8 = undefined;
    reg[0] = 0;
    std.mem.writeInt(u32, reg[1..5], timeout_ms, .little);
    std.mem.writeInt(u32, reg[5..9], max_retries, .little);
    try applyRegister(h, "echo", &reg, 1);
    var inv_buf: [512]u8 = undefined;
    const inv = ActionsHandler.encodeInvokeValue(&inv_buf, "default", "echo", "{}", "", "", "") orelse return error.EncodeFailed;
    try applyInvoke(h, "act-1", inv, 1000 * std.time.ns_per_ms);
    const task = h.claimPendingRun(std.testing.allocator, "default", "echo", null, "w-1") orelse return error.NothingToClaim;
    std.testing.allocator.free(task.body);
    h.runs.getPtr("act-1").?.lease_from_ms = started_at_ms;
}

test "actions: a run past its action's timeout goes back to pending while it has attempts, then times out" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testTimedRun(&h, 1000, 2, 10_000);

    try testing.expectEqual(@as(usize, 0), h.expireRuns(null, 10_999));
    try testing.expectEqual(@as(usize, 1), h.expireRuns(null, 11_000));
    var run = h.runs.get("act-1").?;
    try testing.expectEqual(ActionRunStatus.pending, run.status);
    try testing.expectEqualStrings("timed out after 1000 ms", run.error_owned.?);

    // Its second attempt, out of retries, times out for good.
    const task = h.claimPendingRun(std.testing.allocator, "default", "echo", null, "w-2") orelse return error.NothingToClaim;
    std.testing.allocator.free(task.body);
    h.runs.getPtr("act-1").?.lease_from_ms = 20_000;
    try testing.expectEqual(@as(usize, 1), h.expireRuns(null, 21_000));
    run = h.runs.get("act-1").?;
    try testing.expectEqual(ActionRunStatus.timed_out, run.status);
    try testing.expect(run.completed_at_ms != null);
    // A late report from either worker is stale now.
    var buf: [128]u8 = undefined;
    try testing.expectEqual(proto.StatusCode.conflict, h.handleActionComplete(null, testReport(&buf, .action_complete, "w-2", "act-1", false)).refused.status);
}

test "actions: a timeout of 0 never expires, touched or not, and a re-registered timeout applies to runs in flight" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testTimedRun(&h, 0, 3, 10_000);
    try testing.expectEqual(@as(usize, 0), h.expireRuns(null, 10_000_000));
    var buf: [64]u8 = undefined;
    try testing.expect(h.handleActionTouch(testTouch(&buf, 30_000)) == null);
    try testing.expectEqual(@as(usize, 0), h.expireRuns(null, std.math.maxInt(i64)));
    // As freshly claimed again.
    h.runs.getPtr("act-1").?.lease_from_ms = 10_000;
    h.runs.getPtr("act-1").?.lease_ms = null;

    var reg: [9]u8 = undefined;
    reg[0] = 0;
    std.mem.writeInt(u32, reg[1..5], 500, .little);
    std.mem.writeInt(u32, reg[5..9], 3, .little);
    try applyRegister(&h, "echo", &reg, 2);
    try testing.expectEqual(@as(usize, 1), h.expireRuns(null, 10_500));
}

/// A touch of act-1 by w-1 asking for `extend_ms` (null: none on the wire).
fn testTouch(buf: []u8, extend_ms: ?u32) Request {
    var off: usize = 0;
    for ([_][]const u8{ "echo", "act-1" }) |f| {
        std.mem.writeInt(u16, buf[off..][0..2], @intCast(f.len), .little);
        @memcpy(buf[off + 2 ..][0..f.len], f);
        off += 2 + f.len;
    }
    if (extend_ms) |e| {
        std.mem.writeInt(u32, buf[off..][0..4], e, .little);
        off += 4;
    }
    return .{ .header = proto.RequestHeader.init(.action_touch, 1), .namespace = "default", .key = "w-1", .value = buf[0..off], .options = "" };
}

test "actions: a touch restarts the lease, for the longer of its length and the action's timeout, capped" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testTimedRun(&h, 1000, 3, 10_000);
    var buf: [64]u8 = undefined;

    // No length: the action's timeout from now.
    try testing.expect(h.handleActionTouch(testTouch(&buf, null)) == null);
    var from = h.runs.get("act-1").?.lease_from_ms;
    try testing.expectEqual(@as(usize, 0), h.expireRuns(null, from + 999));

    // A named length shorter than the action's timeout doesn't shorten it.
    try testing.expect(h.handleActionTouch(testTouch(&buf, 500)) == null);
    from = h.runs.get("act-1").?.lease_from_ms;
    try testing.expectEqual(@as(usize, 0), h.expireRuns(null, from + 999));

    // A named length, past the action's timeout.
    try testing.expect(h.handleActionTouch(testTouch(&buf, 5000)) == null);
    from = h.runs.get("act-1").?.lease_from_ms;
    try testing.expectEqual(@as(usize, 0), h.expireRuns(null, from + 4999));

    // Longer than a touch may ask for: refused, the lease unchanged.
    try testing.expectEqual(proto.StatusCode.bad_request, h.handleActionTouch(testTouch(&buf, MAX_TOUCH_EXTEND_MS + 1)).?.status);
    try testing.expectEqual(@as(usize, 1), h.expireRuns(null, from + 5000));
    try testing.expectEqualStrings("timed out after 5000 ms", h.runs.get("act-1").?.error_owned.?);
}

test "actions: a timeout proposed once isn't proposed again until it clears" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testTimedRun(&h, 1000, 3, 10_000);
    // Proposed and not applied (as on a leader): the next sweep skips it;
    // when this node stops leading, a sweep may propose it again.
    h.runs.getPtr("act-1").?.expiry_proposed = true;
    try testing.expectEqual(@as(usize, 0), h.expireRuns(null, 20_000));
    h.forgetExpiries();
    try testing.expectEqual(@as(usize, 1), h.expireRuns(null, 20_000));
}

test "actions: a run of a deleted action ends on the default timeout" {
    var h = ActionsHandler.init(std.testing.allocator);
    defer h.deinit();
    try testTimedRun(&h, 0, 1, 10_000);
    h.replayDelete("echo");
    try testing.expectEqual(@as(usize, 0), h.expireRuns(null, 10_000 + DEFAULT_ACTION_TIMEOUT_MS - 1));
    try testing.expectEqual(@as(usize, 1), h.expireRuns(null, 10_000 + DEFAULT_ACTION_TIMEOUT_MS));
    try testing.expectEqual(ActionRunStatus.timed_out, h.runs.get("act-1").?.status);
}

test "actions: a parked await's key holds a long worker id whole" {
    var buf: [waiter_pool_mod.MAX_WAITER_KEY]u8 = undefined;
    const long_id = comptime @import("stdx").repeat("w", 1000);
    const name = comptime @import("stdx").repeat("a", 256);
    const key = AwaitKey.build(&buf, "orders", name, long_id).?;
    const parts = AwaitKey.parse(key.bytes, key.lengths).?;
    try std.testing.expectEqualStrings("orders", parts.namespace);
    try std.testing.expectEqualStrings(name, parts.action_name);
    try std.testing.expectEqualStrings(long_id, parts.worker_id);
    // Past what a waiter holds: refused, not cut.
    const too_long = comptime @import("stdx").repeat("w", waiter_pool_mod.MAX_WAITER_KEY);
    try std.testing.expect(AwaitKey.build(&buf, "orders", "a", too_long) == null);
}
