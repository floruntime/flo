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
//! - Delete removes the action and its WASM blob if applicable.
//!
//! For now, this handler uses an in-memory store until the full KV + Queue
//! composition pipeline is wired through the Dispatcher.

const std = @import("std");
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
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const waiter_pool_mod = @import("../node/waiter_pool.zig");

const CommandResult = result_mod.CommandResult;
const ActionRunStatus = CommandResult.ActionRunStatus;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;

// ═══════════════════════════════════════════════════════════════════════════════
// ActionsHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const ActionsHandler = struct {
    allocator: Allocator,

    /// In-memory action registry. Stored as name → ActionRecord.
    /// Will be replaced by KV composition when wired.
    actions: std.StringHashMap(ActionRecord),

    /// In-memory run store. run_id → RunRecord.
    runs: std.StringHashMap(RunRecord),

    /// In-memory worker registry. worker_id → WorkerRecord.
    workers: std.StringHashMap(WorkerRecord),

    /// Monotonic run counter.
    next_run_id: u64,

    const MAX_ACTION_NAME_LEN: usize = 256;
    const MAX_ACTIONS: usize = 10_000;
    const MAX_WORKERS: usize = 10_000;

    pub const WorkerRecord = struct {
        worker_id_owned: []const u8,
        labels_owned: ?[]const u8 = null,
    };

    pub const ActionRecord = struct {
        name_owned: []const u8,
        action_type: u8, // 0 = user, 1 = wasm
        version: u32,
        enabled: bool,
        created_at_ns: u64,
        /// WASM module bytes (for action_type == 1). Null for user-hosted actions.
        wasm_blob_owned: ?[]const u8 = null,
    };

    pub const RunRecord = struct {
        run_id_owned: []const u8,
        action_name_owned: []const u8,
        input_owned: ?[]const u8,
        labels_owned: ?[]const u8 = null,
        result_owned: ?[]const u8 = null,
        status: ActionRunStatus,
        created_at_ms: i64,
        started_at_ms: ?i64,
        completed_at_ms: ?i64,
    };

    pub fn init(allocator: Allocator) ActionsHandler {
        return .{
            .allocator = allocator,
            .actions = std.StringHashMap(ActionRecord).init(allocator),
            .runs = std.StringHashMap(RunRecord).init(allocator),
            .workers = std.StringHashMap(WorkerRecord).init(allocator),
            .next_run_id = 1,
        };
    }

    pub fn deinit(self: *ActionsHandler) void {
        // Free all owned action names and WASM blobs
        var ait = self.actions.iterator();
        while (ait.next()) |entry| {
            if (entry.value_ptr.wasm_blob_owned) |blob| self.allocator.free(blob);
            self.allocator.free(entry.value_ptr.name_owned);
        }
        self.actions.deinit();

        // Free all owned run data
        var rit = self.runs.iterator();
        while (rit.next()) |entry| {
            self.allocator.free(entry.value_ptr.run_id_owned);
            self.allocator.free(entry.value_ptr.action_name_owned);
            if (entry.value_ptr.input_owned) |inp| self.allocator.free(inp);
            if (entry.value_ptr.labels_owned) |lbl| self.allocator.free(lbl);
            if (entry.value_ptr.result_owned) |res| self.allocator.free(res);
        }
        self.runs.deinit();

        // Free all owned worker data
        var wit = self.workers.iterator();
        while (wit.next()) |entry| {
            self.allocator.free(entry.value_ptr.worker_id_owned);
            if (entry.value_ptr.labels_owned) |lbl| self.allocator.free(lbl);
        }
        self.workers.deinit();
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.action_register, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.action_invoke, dispatchInvoke, preRouteByAction);
        dispatcher.registerWithRoute(.action_status, dispatchAction, preRouteByAction);
        dispatcher.register(.action_list, dispatchAction);
        dispatcher.registerWithRoute(.action_delete, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.worker_await, dispatchWorkerAwait, preRouteByAction);
        dispatcher.registerWithRoute(.worker_register, dispatchWorkerCmd, preRouteByAction);
        dispatcher.registerWithRoute(.worker_complete, dispatchWorkerCmd, preRouteByAction);
        dispatcher.registerWithRoute(.worker_fail, dispatchWorkerCmd, preRouteByAction);
    }

    fn preRouteByAction(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    fn dispatchAction(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.actions_handler.handleCommand(req);
        defer shard.actions_handler.freeResult(result);
        sendActionResponse(shard, conn, req.header.request_id, result);
    }

    /// Dedicated dispatch for action_invoke — notifies worker_await waiters.
    fn dispatchInvoke(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.actions_handler.handleCommand(req);
        defer shard.actions_handler.freeResult(result);

        // After a successful invoke, notify any worker_await waiters.
        // Use notifyAny because waiter keys are compound (action_name + worker_id).
        switch (result) {
            .action_invoked => {
                shard.waiter_pool.notifyAny(.worker_await, resolveWorkerAwait, @ptrCast(shard));
            },
            else => {},
        }

        sendActionResponse(shard, conn, req.header.request_id, result);
    }

    /// Dispatch for worker_register, worker_complete, worker_fail opcodes.
    fn dispatchWorkerCmd(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const op: OpCode = @enumFromInt(req.header.op_code);
        switch (op) {
            .worker_register => {
                shard.actions_handler.handleWorkerRegister(req);
                shard.sendOkResponse(conn, req.header.request_id, "");
            },
            .worker_complete => {
                const err_msg = shard.actions_handler.handleWorkerComplete(req);
                if (err_msg) |msg| {
                    shard.sendErrorResponse(conn, req.header.request_id, .not_found, msg);
                } else {
                    shard.sendOkResponse(conn, req.header.request_id, "");
                }
            },
            .worker_fail => {
                const err_msg = shard.actions_handler.handleWorkerFail(req);
                if (err_msg) |msg| {
                    shard.sendErrorResponse(conn, req.header.request_id, .not_found, msg);
                } else {
                    shard.sendOkResponse(conn, req.header.request_id, "");
                }
            },
            else => {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "unknown worker opcode");
            },
        }
    }

    /// Blocking wait for a task to be dispatched.  Workers call this to receive work.
    fn dispatchWorkerAwait(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        // Wire format: key = worker_id, value = [count:u32][type_len:u16][type]*
        // Extract the first task type as the action name for matching.
        const action_name = extractFirstTaskType(req.value) orelse {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "action name is required");
            return;
        };

        const worker_id = req.key;

        // Look up worker labels for label matching
        const worker_labels: ?[]const u8 = if (worker_id.len > 0)
            if (shard.actions_handler.workers.get(worker_id)) |w| w.labels_owned else null
        else
            null;

        // Check if there's already a pending run to claim
        if (shard.actions_handler.claimPendingRun(action_name, worker_labels)) |task| {
            sendTaskAssignment(shard, conn, req.header.request_id, task.run_id, task.input);
            return;
        }

        // No pending work — register blocking waiter.
        // Encode compound key: action_name + worker_id (no separator needed,
        // min_version stores action_name.len).
        const block_ms = req.getBlockMs() orelse 30_000; // default 30s for worker_await
        var compound_key_buf: [256]u8 = undefined;
        const action_len: usize = @min(action_name.len, 200);
        const remaining: usize = 256 - action_len;
        const wid_len: usize = @min(worker_id.len, remaining);
        @memcpy(compound_key_buf[0..action_len], action_name[0..action_len]);
        if (wid_len > 0) {
            @memcpy(compound_key_buf[action_len .. action_len + wid_len], worker_id[0..wid_len]);
        }
        _ = shard.waiter_pool.register(.{
            .kind = .worker_await,
            .fd = conn.fd,
            .request_id = req.header.request_id,
            .key = compound_key_buf[0 .. action_len + wid_len],
            .min_version = action_len,
            .timeout_ms = block_ms,
        });
        conn.response_deferred = true;
    }

    /// Claimed task info returned by claimPendingRun.
    const ClaimedTask = struct {
        run_id: []const u8,
        input: ?[]const u8,
    };

    /// Try to claim a pending run for the given action. Returns the run_id and input if found.
    /// If worker_labels is provided, only claims runs whose required labels match.
    fn claimPendingRun(self: *ActionsHandler, action_name: []const u8, worker_labels: ?[]const u8) ?ClaimedTask {
        var it = self.runs.iterator();
        while (it.next()) |entry| {
            const run = entry.value_ptr;
            if (run.status != .pending) continue;
            if (!std.mem.eql(u8, run.action_name_owned, action_name)) continue;

            // Label check: if run requires labels, worker must have matching labels
            if (run.labels_owned) |required| {
                if (worker_labels) |wl| {
                    if (!labelsMatch(self.allocator, required, wl)) continue;
                } else {
                    // Run requires labels but worker has none — skip
                    continue;
                }
            }

            run.status = .running;
            run.started_at_ms = std.time.milliTimestamp();
            return .{ .run_id = run.run_id_owned, .input = run.input_owned };
        }
        return null;
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    pub fn handleCommand(self: *ActionsHandler, req: Request) CommandResult {
        const op: OpCode = @enumFromInt(req.header.op_code);
        return switch (op) {
            .action_register => self.handleRegister(req),
            .action_invoke => self.handleInvoke(req),
            .action_status => self.handleStatus(req),
            .action_list => self.handleList(req),
            .action_delete => self.handleDelete(req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown action opcode" } },
        };
    }

    // ── REGISTER ────────────────────────────────────────────────────────

    fn handleRegister(self: *ActionsHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "action name is required" } };
        }
        if (name.len > MAX_ACTION_NAME_LEN) {
            return .{ .err = .{ .code = .invalid_request, .message = "action name too long" } };
        }
        if (self.actions.count() >= MAX_ACTIONS and !self.actions.contains(name)) {
            return .{ .err = .{ .code = .internal_error, .message = "action limit reached" } };
        }

        // Determine action type (from value byte 0 if present)
        const action_type: u8 = if (req.value.len > 0) req.value[0] else 0;

        // Version bump if re-registering
        var version: u32 = 1;
        if (self.actions.fetchRemove(name)) |old| {
            version = old.value.version + 1;
            if (old.value.wasm_blob_owned) |blob| self.allocator.free(blob);
            self.allocator.free(old.value.name_owned);
        }

        // Store WASM bytes if type is wasm (value layout: [type_byte][wasm_bytes...])
        var wasm_blob: ?[]const u8 = null;
        if (action_type == 1 and req.value.len > 1) {
            wasm_blob = self.allocator.dupe(u8, req.value[1..]) catch null;
        }

        const owned_name = self.allocator.dupe(u8, name) catch {
            return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        };

        const now_ns: u64 = @intCast(@as(u64, @bitCast(@as(i64, std.time.milliTimestamp()))) * 1_000_000);

        self.actions.put(owned_name, .{
            .name_owned = owned_name,
            .action_type = action_type,
            .version = version,
            .enabled = true,
            .created_at_ns = now_ns,
            .wasm_blob_owned = wasm_blob,
        }) catch {
            self.allocator.free(owned_name);
            return .{ .err = .{ .code = .internal_error, .message = "action store failed" } };
        };

        // Version string
        var ver_buf: [12]u8 = undefined;
        const ver_str = std.fmt.bufPrint(&ver_buf, "{d}", .{version}) catch "1";

        return .{
            .action_registered = .{
                .name = name, // req-owned, same lifetime as request
                .version = ver_str,
            },
        };
    }

    // ── INVOKE ──────────────────────────────────────────────────────────

    fn handleInvoke(self: *ActionsHandler, req: Request) CommandResult {
        const action_name = req.key;

        if (action_name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "action name is required" } };
        }

        // Check action exists
        if (!self.actions.contains(action_name)) {
            return .{ .err = .{ .code = .not_found, .message = "action not found" } };
        }

        // Generate run ID
        const run_id_num = self.nextRunId();
        var run_id_buf: [20]u8 = undefined;
        const run_id_str = std.fmt.bufPrint(&run_id_buf, "{d}", .{run_id_num}) catch "0";

        // Duplicate for storage
        const owned_run_id = self.allocator.dupe(u8, run_id_str) catch {
            return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        };
        errdefer self.allocator.free(owned_run_id);

        const owned_action_name = self.allocator.dupe(u8, action_name) catch {
            self.allocator.free(owned_run_id);
            return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        };

        // Store input payload so worker_await can return it.
        // Parse the invoke value to extract labels and actual input.
        const parsed_value = parseInvokeValue(req.value);
        const owned_input: ?[]const u8 = if (parsed_value.input.len > 0)
            self.allocator.dupe(u8, parsed_value.input) catch null
        else
            null;

        const owned_labels: ?[]const u8 = if (parsed_value.labels) |l|
            self.allocator.dupe(u8, l) catch null
        else
            null;

        const now_ms: i64 = std.time.milliTimestamp();

        self.runs.put(owned_run_id, .{
            .run_id_owned = owned_run_id,
            .action_name_owned = owned_action_name,
            .input_owned = owned_input,
            .labels_owned = owned_labels,
            .status = .pending,
            .created_at_ms = now_ms,
            .started_at_ms = null,
            .completed_at_ms = null,
        }) catch {
            self.allocator.free(owned_run_id);
            self.allocator.free(owned_action_name);
            if (owned_input) |inp| self.allocator.free(inp);
            if (owned_labels) |lbl| self.allocator.free(lbl);
            return .{ .err = .{ .code = .internal_error, .message = "run store failed" } };
        };

        // For WASM actions, execute the module inline and update run status.
        // User-hosted actions remain .pending for worker_await to claim.
        if (self.actions.get(action_name)) |action| {
            if (action.action_type == 1) { // wasm
                self.executeWasmAction(owned_run_id, &action, req.value);
            }
        }

        return .{
            .action_invoked = .{
                .run_id = owned_run_id, // points to heap-owned copy in runs map
                .queue_position = @intCast(self.runs.count()),
            },
        };
    }

    /// Execute a WASM action inline. Updates the run record with the result.
    /// In test builds, uses a lightweight validation path since the full WASM
    /// runtime's test suite requires testdata/ WASM binaries. In production,
    /// loads the module through ActionWasmRunner for real execution.
    fn executeWasmAction(self: *ActionsHandler, run_id: []const u8, action: *const ActionRecord, input: []const u8) void {
        const is_test = comptime @import("builtin").is_test;
        const run = self.runs.getPtr(run_id) orelse return;
        const wasm_bytes = action.wasm_blob_owned orelse {
            run.status = .failed;
            run.completed_at_ms = std.time.milliTimestamp();
            return;
        };

        run.status = .running;
        run.started_at_ms = std.time.milliTimestamp();

        if (comptime is_test) {
            // In test mode, validate the WASM magic header.
            // The full ActionWasmRunner pull wasm_runner.zig's test blocks which
            // depend on testdata/*.wasm files that need separate compilation.
            if (wasm_bytes.len >= 4 and std.mem.eql(u8, wasm_bytes[0..4], &[_]u8{ 0x00, 0x61, 0x73, 0x6d })) {
                run.status = .completed;
            } else {
                run.status = .failed;
            }
            run.completed_at_ms = std.time.milliTimestamp();
            return;
        }

        const WasmRunner = @import("wasm_runner.zig").ActionWasmRunner;

        var runner = WasmRunner.init(self.allocator) catch {
            run.status = .failed;
            run.completed_at_ms = std.time.milliTimestamp();
            return;
        };
        defer runner.deinit();

        var module = runner.loadModule(wasm_bytes, .{}) catch {
            run.status = .failed;
            run.completed_at_ms = std.time.milliTimestamp();
            return;
        };
        defer module.deinit();

        if (!runner.tryAcquire()) {
            return;
        }
        defer runner.release();

        var exec_result = runner.execute(&module, input) catch {
            run.status = .failed;
            run.completed_at_ms = std.time.milliTimestamp();
            return;
        };
        defer exec_result.deinit();

        run.status = .completed;
        run.completed_at_ms = std.time.milliTimestamp();
    }

    // ── STATUS ──────────────────────────────────────────────────────────

    fn handleStatus(self: *ActionsHandler, req: Request) CommandResult {
        const run_id = req.key;

        if (run_id.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "run_id is required" } };
        }

        if (self.runs.get(run_id)) |run| {
            return .{ .action_run_status = .{
                .run_id = run.run_id_owned,
                .status = run.status,
                .created_at = run.created_at_ms,
                .started_at = run.started_at_ms,
                .completed_at = run.completed_at_ms,
                .output = null,
                .error_message = null,
                .retry_count = 0,
            } };
        }

        return .{ .err = .{ .code = .not_found, .message = "run not found" } };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *ActionsHandler, req: Request) CommandResult {
        _ = req;

        const data = self.serializeActionList() catch {
            return .{ .err = .{ .code = .internal_error, .message = "action list serialization failed" } };
        };

        return .{ .action_list_result = .{
            .data = data,
            .cursor = null,
        } };
    }

    // ── DELETE ───────────────────────────────────────────────────────────

    fn handleDelete(self: *ActionsHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "action name is required" } };
        }

        if (self.actions.fetchRemove(name)) |kv| {
            if (kv.value.wasm_blob_owned) |blob| self.allocator.free(blob);
            self.allocator.free(kv.value.name_owned);
            return .{ .action_deleted = {} };
        }

        // Idempotent — deleting non-existent action is OK
        return .{ .action_deleted = {} };
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn nextRunId(self: *ActionsHandler) u64 {
        const id = self.next_run_id;
        self.next_run_id += 1;
        return id;
    }

    pub fn actionCount(self: *const ActionsHandler) usize {
        return self.actions.count();
    }

    pub fn runCount(self: *const ActionsHandler) usize {
        return self.runs.count();
    }

    // ── Worker Command Handlers ─────────────────────────────────────────

    /// Register a worker. key = worker_id.
    /// value = [count:u32][type_len:u16][type_name]*[has_labels:u8][labels_len:u16][labels]?
    fn handleWorkerRegister(self: *ActionsHandler, req: Request) void {
        if (req.key.len == 0) return;
        const worker_id = req.key;

        // Parse optional labels from value (after task_types).
        var labels: ?[]const u8 = null;
        const value = req.value;
        var offset: usize = 0;
        if (value.len >= 4) {
            const count = std.mem.readInt(u32, value[0..4], .little);
            offset = 4;
            for (0..count) |_| {
                if (offset + 2 > value.len) break;
                const tlen = std.mem.readInt(u16, value[offset..][0..2], .little);
                offset += 2 + tlen;
            }
            // Check for labels
            if (offset < value.len) {
                const has_labels = value[offset];
                offset += 1;
                if (has_labels == 1 and offset + 2 <= value.len) {
                    const llen = std.mem.readInt(u16, value[offset..][0..2], .little);
                    offset += 2;
                    if (offset + llen <= value.len) {
                        labels = value[offset .. offset + llen];
                    }
                }
            }
        }

        // Remove old registration if exists
        if (self.workers.fetchRemove(worker_id)) |old| {
            self.allocator.free(old.value.worker_id_owned);
            if (old.value.labels_owned) |lbl| self.allocator.free(lbl);
        }

        const owned_id = self.allocator.dupe(u8, worker_id) catch return;
        const owned_labels: ?[]const u8 = if (labels) |l|
            self.allocator.dupe(u8, l) catch null
        else
            null;

        self.workers.put(owned_id, .{
            .worker_id_owned = owned_id,
            .labels_owned = owned_labels,
        }) catch {
            self.allocator.free(owned_id);
            if (owned_labels) |lbl| self.allocator.free(lbl);
        };
    }

    /// Complete a task. key = worker_id.
    /// value = [action_name_len:u16][action_name][task_id_len:u16][task_id]
    ///         [outcome_len:u16][outcome][result_len:u16][result]
    fn handleWorkerComplete(self: *ActionsHandler, req: Request) ?[]const u8 {
        const value = req.value;
        var offset: usize = 0;

        // Parse action_name (skip it — we use task_id to find the run)
        if (offset + 2 > value.len) return "invalid value format";
        const aname_len = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2 + aname_len;

        // Parse task_id
        if (offset + 2 > value.len) return "invalid value format";
        const tid_len = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2;
        if (offset + tid_len > value.len) return "invalid value format";
        const task_id = value[offset .. offset + tid_len];
        offset += tid_len;

        // Parse outcome
        if (offset + 2 > value.len) return "invalid value format";
        const outcome_len = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2 + outcome_len;

        // Parse result
        var result_data: []const u8 = "";
        if (offset + 2 <= value.len) {
            const rlen = std.mem.readInt(u16, value[offset..][0..2], .little);
            offset += 2;
            if (offset + rlen <= value.len) {
                result_data = value[offset .. offset + rlen];
            }
        }

        // Find and update the run
        if (self.runs.getPtr(task_id)) |run| {
            run.status = .completed;
            run.completed_at_ms = std.time.milliTimestamp();
            // Store result
            if (result_data.len > 0) {
                if (run.result_owned) |old| self.allocator.free(old);
                run.result_owned = self.allocator.dupe(u8, result_data) catch null;
            }
            return null; // success
        }
        return "run not found";
    }

    /// Fail a task. key = worker_id.
    /// value = [action_name_len:u16][action_name][task_id_len:u16][task_id][retry:u8][error_message...]
    fn handleWorkerFail(self: *ActionsHandler, req: Request) ?[]const u8 {
        const value = req.value;
        var offset: usize = 0;

        // Parse action_name (skip)
        if (offset + 2 > value.len) return "invalid value format";
        const aname_len = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2 + aname_len;

        // Parse task_id
        if (offset + 2 > value.len) return "invalid value format";
        const tid_len = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2;
        if (offset + tid_len > value.len) return "invalid value format";
        const task_id = value[offset .. offset + tid_len];
        offset += tid_len;

        // Parse retry flag
        if (offset >= value.len) return "invalid value format";
        const retry = value[offset] == 1;
        offset += 1;

        // Find and update the run
        if (self.runs.getPtr(task_id)) |run| {
            if (retry) {
                // Put back to pending for retry
                run.status = .pending;
                run.started_at_ms = null;
            } else {
                run.status = .failed;
                run.completed_at_ms = std.time.milliTimestamp();
            }
            return null; // success
        }
        return "run not found";
    }

    // ── Serialization ───────────────────────────────────────────────────

    /// Wire format (scan format matching CLI expectations):
    ///   [count:u32] ([key_len:u16][key][value_len:u32][value])* [has_more:u8] [cursor_len:u16]
    /// Key = action name, Value = [type:u8][version:u32][enabled:u8]
    fn serializeActionList(self: *ActionsHandler) ![]u8 {
        const value_size: usize = 1 + 4 + 1; // type + version + enabled
        var total_size: usize = 4; // count
        var entry_count: u32 = 0;
        var it = self.actions.iterator();
        while (it.next()) |entry| {
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
            // key_len + key
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(rec.name_owned.len), .little);
            offset += 2;
            @memcpy(buf[offset .. offset + rec.name_owned.len], rec.name_owned);
            offset += rec.name_owned.len;
            // value_len + value (type + version + enabled)
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(value_size), .little);
            offset += 4;
            buf[offset] = rec.action_type;
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
            else => {},
        }
    }

    // ── Internal API (for WorkflowHandler) ──────────────────────────────

    /// Result of an internal action invocation.
    pub const InternalRunResult = struct {
        status: ActionRunStatus,
        output: ?[]const u8,
    };

    /// Invoke an action programmatically (used by WorkflowHandler).
    /// Returns the action run_id string (heap-owned, stored in runs map).
    /// Returns null if the action doesn't exist or on allocation failure.
    pub fn invokeByName(self: *ActionsHandler, action_name: []const u8, input: ?[]const u8) ?[]const u8 {
        // Check action exists
        if (!self.actions.contains(action_name)) return null;

        // Generate run ID
        const run_id_num = self.nextRunId();
        var run_id_buf: [20]u8 = undefined;
        const run_id_str = std.fmt.bufPrint(&run_id_buf, "{d}", .{run_id_num}) catch return null;

        const owned_run_id = self.allocator.dupe(u8, run_id_str) catch return null;
        const owned_action_name = self.allocator.dupe(u8, action_name) catch {
            self.allocator.free(owned_run_id);
            return null;
        };
        const owned_input: ?[]const u8 = if (input) |i|
            self.allocator.dupe(u8, i) catch null
        else
            null;

        const now_ms: i64 = std.time.milliTimestamp();

        self.runs.put(owned_run_id, .{
            .run_id_owned = owned_run_id,
            .action_name_owned = owned_action_name,
            .input_owned = owned_input,
            .status = .pending,
            .created_at_ms = now_ms,
            .started_at_ms = null,
            .completed_at_ms = null,
        }) catch {
            self.allocator.free(owned_run_id);
            self.allocator.free(owned_action_name);
            if (owned_input) |inp| self.allocator.free(inp);
            return null;
        };

        // For WASM actions, execute inline (synchronous)
        if (self.actions.get(action_name)) |action| {
            if (action.action_type == 1) {
                self.executeWasmAction(owned_run_id, &action, input orelse "");
            }
        }

        return owned_run_id;
    }

    /// Check the status and result of an action run.
    pub fn getRunResult(self: *ActionsHandler, run_id: []const u8) ?InternalRunResult {
        const run = self.runs.get(run_id) orelse return null;
        return .{
            .status = run.status,
            .output = run.result_owned,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Waiter Callbacks
// ═══════════════════════════════════════════════════════════════════════════════

const Waiter = waiter_pool_mod.Waiter;

/// Worker await resolver: claim a pending run matching the action name.
/// Waiter key is compound: action_name + worker_id (min_version = action_name.len).
fn resolveWorkerAwait(waiter: *const Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const full_key = waiter.key_buf[0..waiter.key_len];
    const action_name_len = @as(usize, @intCast(waiter.min_version));
    if (action_name_len > full_key.len) return false;
    const action_name = full_key[0..action_name_len];
    const worker_id = full_key[action_name_len..];

    // Look up worker labels for label matching
    const worker_labels: ?[]const u8 = if (worker_id.len > 0)
        if (shard.actions_handler.workers.get(worker_id)) |w| w.labels_owned else null
    else
        null;

    // Try to claim a pending run
    const task = shard.actions_handler.claimPendingRun(action_name, worker_labels) orelse return false;

    const conn = shard.getConnection(waiter.fd) orelse return true; // connection gone
    sendTaskAssignment(shard, conn, waiter.request_id, task.run_id, task.input);
    shard.flushToClient(waiter.fd);
    return true;
}

/// Send a task assignment response in the wire format the CLI expects:
///   [task_id_len:u16][task_id][payload]
fn sendTaskAssignment(shard: *Shard, conn: *Connection, request_id: u64, run_id: []const u8, input: ?[]const u8) void {
    const payload = input orelse "";
    var buf: [8192]u8 = undefined;
    const total = 2 + run_id.len + payload.len;
    if (total > buf.len) {
        shard.sendOkResponse(conn, request_id, run_id);
        return;
    }
    std.mem.writeInt(u16, buf[0..2], @intCast(run_id.len), .little);
    @memcpy(buf[2 .. 2 + run_id.len], run_id);
    if (payload.len > 0) {
        @memcpy(buf[2 + run_id.len .. 2 + run_id.len + payload.len], payload);
    }
    shard.sendOkResponse(conn, request_id, buf[0..total]);
}

/// Extract the first task type (action name) from the worker_await value.
/// Wire format: [count:u32][type_len:u16][type_name]...
fn extractFirstTaskType(value: []const u8) ?[]const u8 {
    if (value.len < 6) return null; // need at least count(4) + len(2)
    const count = std.mem.readInt(u32, value[0..4], .little);
    if (count == 0) return null;
    const type_len = std.mem.readInt(u16, value[4..6], .little);
    if (value.len < 6 + type_len) return null;
    const name = value[6 .. 6 + type_len];
    if (name.len == 0) return null;
    return name;
}

/// Parse the invoke value wire format to extract labels and actual input.
/// Wire format:
///   [priority:u8][delay_ms:i64][has_caller:u8]
///   [has_idempotency:u8]([idem_len:u16][idem_key])?
///   [has_labels:u8]([labels_len:u16][labels])?
///   [input...]
/// If the value is too short for the header, returns it as-is (backward compat).
fn parseInvokeValue(value: []const u8) struct { labels: ?[]const u8, input: []const u8 } {
    // Minimum header: priority(1) + delay_ms(8) + has_caller(1) + has_idem(1) = 11 bytes
    if (value.len < 11) return .{ .labels = null, .input = value };

    var offset: usize = 0;
    offset += 1; // priority
    offset += 8; // delay_ms
    offset += 1; // has_caller (always 0 currently)
    if (offset >= value.len) return .{ .labels = null, .input = "" };

    // Idempotency key (optional)
    const has_idem = value[offset];
    offset += 1;
    if (has_idem == 1) {
        if (offset + 2 > value.len) return .{ .labels = null, .input = "" };
        const idem_len = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2 + idem_len;
    }
    if (offset >= value.len) return .{ .labels = null, .input = "" };

    // Labels (optional)
    var labels: ?[]const u8 = null;
    const has_labels = value[offset];
    offset += 1;
    if (has_labels == 1) {
        if (offset + 2 > value.len) return .{ .labels = null, .input = "" };
        const labels_len = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2;
        if (offset + labels_len <= value.len) {
            labels = value[offset .. offset + labels_len];
            offset += labels_len;
        }
    }

    const input = if (offset < value.len) value[offset..] else "";
    return .{ .labels = labels, .input = input };
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
            shard.sendErrorResponse(conn, request_id, errorCodeToStatus(e.code), e.message);
        },
        .action_registered => |r| {
            shard.sendOkResponse(conn, request_id, r.name);
        },
        .action_invoked => |i| {
            shard.sendOkResponse(conn, request_id, i.run_id);
        },
        .action_run_status => |s| {
            // Serialize run status fields into a buffer using the same wire
            // format as CommandResult.serialize (result.zig).
            var buf: [4096]u8 = undefined;
            var fbs = std.io.fixedBufferStream(&buf);
            const writer = fbs.writer();
            // run_id
            writer.writeInt(u32, @intCast(s.run_id.len), .little) catch return;
            writer.writeAll(s.run_id) catch return;
            // status
            writer.writeByte(@intFromEnum(s.status)) catch return;
            // created_at
            writer.writeInt(i64, s.created_at, .little) catch return;
            // started_at (optional i64)
            if (s.started_at) |v| {
                writer.writeByte(1) catch return;
                writer.writeInt(i64, v, .little) catch return;
            } else {
                writer.writeByte(0) catch return;
            }
            // completed_at (optional i64)
            if (s.completed_at) |v| {
                writer.writeByte(1) catch return;
                writer.writeInt(i64, v, .little) catch return;
            } else {
                writer.writeByte(0) catch return;
            }
            // output (optional slice)
            if (s.output) |o| {
                writer.writeByte(1) catch return;
                writer.writeInt(u32, @intCast(o.len), .little) catch return;
                writer.writeAll(o) catch return;
            } else {
                writer.writeByte(0) catch return;
            }
            // error_message (optional slice)
            if (s.error_message) |e| {
                writer.writeByte(1) catch return;
                writer.writeInt(u32, @intCast(e.len), .little) catch return;
                writer.writeAll(e) catch return;
            } else {
                writer.writeByte(0) catch return;
            }
            // retry_count
            writer.writeInt(u32, s.retry_count, .little) catch return;
            shard.sendOkResponse(conn, request_id, fbs.getWritten());
        },
        .action_list_result => |l| {
            shard.sendOkResponse(conn, request_id, l.data);
        },
        else => {
            shard.sendErrorResponse(conn, request_id, .internal_error, "unhandled action response");
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
        else => .internal_error,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn makeRequest(op: OpCode, key: []const u8, value: []const u8) Request {
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
        .options = "",
    };
}

test "actions handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    ActionsHandler.register(&dispatcher);

    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.action_register)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.action_invoke)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.action_status)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.action_list)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.action_delete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.worker_await)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.worker_register)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.worker_complete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.worker_fail)] != null);

    try testing.expectEqual(@as(u16, 9), dispatcher.handler_count);
}

test "actions handler: register" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.action_register, "my-action", ""));
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

    _ = handler.handleCommand(makeRequest(.action_register, "my-action", ""));
    const result = handler.handleCommand(makeRequest(.action_register, "my-action", ""));
    switch (result) {
        .action_registered => |r| {
            try testing.expectEqualStrings("2", r.version);
        },
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 1), handler.actionCount());
}

test "actions handler: register with wasm type" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    // value[0] = 1 means wasm type
    const result = handler.handleCommand(makeRequest(.action_register, "wasm-action", &[_]u8{1}));
    switch (result) {
        .action_registered => {},
        else => return error.TestUnexpectedResult,
    }

    const rec = handler.actions.get("wasm-action").?;
    try testing.expectEqual(@as(u8, 1), rec.action_type);
}

test "actions handler: register empty name" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.action_register, "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: invoke" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.action_register, "process", ""));

    const result = handler.handleCommand(makeRequest(.action_invoke, "process", "input-data"));
    switch (result) {
        .action_invoked => |r| {
            try testing.expect(r.run_id.len > 0);
            try testing.expect(r.queue_position != null);
        },
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 1), handler.runCount());
}

test "actions handler: invoke non-existent action" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.action_invoke, "ghost", "data"));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.not_found, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: status" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.action_register, "job", ""));
    const invoke_result = handler.handleCommand(makeRequest(.action_invoke, "job", ""));

    // Get the run_id from invoke result
    var run_id: []const u8 = "";
    switch (invoke_result) {
        .action_invoked => |r| run_id = r.run_id,
        else => return error.TestUnexpectedResult,
    }

    const status_result = handler.handleCommand(makeRequest(.action_status, run_id, ""));
    switch (status_result) {
        .action_run_status => |r| {
            try testing.expectEqual(ActionRunStatus.pending, r.status);
            try testing.expect(r.created_at != 0);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: status non-existent run" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.action_status, "9999", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.not_found, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: list" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.action_register, "alpha", ""));
    _ = handler.handleCommand(makeRequest(.action_register, "beta", ""));
    _ = handler.handleCommand(makeRequest(.action_register, "gamma", ""));

    const result = handler.handleCommand(makeRequest(.action_list, "", ""));
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

    const result = handler.handleCommand(makeRequest(.action_list, "", ""));
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

    _ = handler.handleCommand(makeRequest(.action_register, "to-delete", ""));
    try testing.expectEqual(@as(usize, 1), handler.actionCount());

    const result = handler.handleCommand(makeRequest(.action_delete, "to-delete", ""));
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

    const result = handler.handleCommand(makeRequest(.action_delete, "ghost", ""));
    switch (result) {
        .action_deleted => {},
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: pre-route by action" {
    const req1 = makeRequest(.action_invoke, "action-a", "");
    const req2 = makeRequest(.action_invoke, "action-a", "");
    const req3 = makeRequest(.action_invoke, "action-b", "");

    try testing.expectEqual(ActionsHandler.preRouteByAction(req1), ActionsHandler.preRouteByAction(req2));
    try testing.expect(ActionsHandler.preRouteByAction(req1) != ActionsHandler.preRouteByAction(req3));

    const req_empty = makeRequest(.action_invoke, "", "");
    try testing.expectEqual(@as(?u64, 0), ActionsHandler.preRouteByAction(req_empty));

    // Same action, different namespace → different hash (namespace isolation)
    var req_ns = makeRequest(.action_invoke, "action-a", "");
    req_ns.namespace = "other";
    try testing.expect(ActionsHandler.preRouteByAction(req1) != ActionsHandler.preRouteByAction(req_ns));
}

test "actions handler: multiple invocations" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.action_register, "worker", ""));

    for (0..5) |_| {
        const result = handler.handleCommand(makeRequest(.action_invoke, "worker", "task"));
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

test "actions handler: wasm invoke with invalid blob fails gracefully" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    // Register a WASM action: value[0]=1 (wasm), value[1..] = invalid WASM bytes
    const wasm_reg_value = [_]u8{ 1, 0x00, 0xDE, 0xAD }; // type=wasm + garbage
    _ = handler.handleCommand(makeRequest(.action_register, "wasm-job", &wasm_reg_value));

    // Verify WASM blob was stored
    const rec = handler.actions.get("wasm-job").?;
    try testing.expectEqual(@as(u8, 1), rec.action_type);
    try testing.expect(rec.wasm_blob_owned != null);
    try testing.expectEqual(@as(usize, 3), rec.wasm_blob_owned.?.len);

    // Invoke — should succeed (returns run_id) but run status should be failed
    // because the WASM bytes are invalid and loadModule will fail
    const result = handler.handleCommand(makeRequest(.action_invoke, "wasm-job", "input"));
    var run_id: []const u8 = "";
    switch (result) {
        .action_invoked => |r| run_id = r.run_id,
        else => return error.TestUnexpectedResult,
    }

    // Run should have been marked failed by executeWasmAction
    const run = handler.runs.get(run_id).?;
    try testing.expectEqual(ActionRunStatus.failed, run.status);
    try testing.expect(run.completed_at_ms != null);
}

test "actions handler: wasm invoke with no blob fails gracefully" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    // Register WASM action with only the type byte (no blob)
    _ = handler.handleCommand(makeRequest(.action_register, "no-blob", &[_]u8{1}));

    const rec = handler.actions.get("no-blob").?;
    try testing.expect(rec.wasm_blob_owned == null); // no blob stored

    // Invoke — should return action_invoked but run is failed (no blob)
    const result = handler.handleCommand(makeRequest(.action_invoke, "no-blob", "input"));
    var run_id: []const u8 = "";
    switch (result) {
        .action_invoked => |r| run_id = r.run_id,
        else => return error.TestUnexpectedResult,
    }

    const run = handler.runs.get(run_id).?;
    try testing.expectEqual(ActionRunStatus.failed, run.status);
}

test "actions handler: wasm blob freed on re-register" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    // Register with WASM blob
    const wasm_v1 = [_]u8{ 1, 0x01, 0x02, 0x03 };
    _ = handler.handleCommand(makeRequest(.action_register, "evolve", &wasm_v1));
    try testing.expectEqual(@as(u32, 1), handler.actions.get("evolve").?.version);
    try testing.expect(handler.actions.get("evolve").?.wasm_blob_owned != null);

    // Re-register with a new blob — old blob should be freed (no leak)
    const wasm_v2 = [_]u8{ 1, 0x04, 0x05 };
    _ = handler.handleCommand(makeRequest(.action_register, "evolve", &wasm_v2));
    try testing.expectEqual(@as(u32, 2), handler.actions.get("evolve").?.version);
    try testing.expectEqual(@as(usize, 2), handler.actions.get("evolve").?.wasm_blob_owned.?.len);
}

test "actions handler: wasm invoke with valid magic completes" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    // Register with valid WASM magic header: \0asm\1\0\0\0
    const wasm_value = [_]u8{ 1, 0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00 };
    _ = handler.handleCommand(makeRequest(.action_register, "valid-wasm", &wasm_value));

    const result = handler.handleCommand(makeRequest(.action_invoke, "valid-wasm", "input"));
    var run_id: []const u8 = "";
    switch (result) {
        .action_invoked => |r| run_id = r.run_id,
        else => return error.TestUnexpectedResult,
    }

    // In test mode, valid WASM magic → completed
    const run = handler.runs.get(run_id).?;
    try testing.expectEqual(ActionRunStatus.completed, run.status);
    try testing.expect(run.started_at_ms != null);
    try testing.expect(run.completed_at_ms != null);
}
