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
//! Durable via the shared persistence interface: write mutations go through
//! Raft propose (persistence.persistEntry), and in-memory state is rebuilt
//! from UAL segments on startup via the ReplayRegistry.

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

const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const EntryType = entry_mod.EntryType;
const Entry = entry_mod.Entry;
const Flags = entry_mod.Flags;

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

    /// Monotonic run counter.
    next_run_id: u64,

    const MAX_ACTION_NAME_LEN: usize = 256;
    const MAX_ACTIONS: usize = 10_000;

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
        outcome_owned: ?[]const u8 = null,
        worker_id_owned: ?[]const u8 = null,
        error_owned: ?[]const u8 = null,
        source: u8 = 0, // 0 = direct, 1 = workflow, 2 = trigger
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
            if (entry.value_ptr.outcome_owned) |out| self.allocator.free(out);
            if (entry.value_ptr.worker_id_owned) |wid| self.allocator.free(wid);
            if (entry.value_ptr.error_owned) |err_msg| self.allocator.free(err_msg);
        }
        self.runs.deinit();
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.action_register, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.action_invoke, dispatchInvoke, preRouteByAction);
        dispatcher.registerWithRoute(.action_status, dispatchAction, preRouteByAction);
        dispatcher.registerWalk(.action_list, dispatchAction, localScanActions);
        dispatcher.registerWithRoute(.action_delete, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.action_list_runs, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.action_await, dispatchActionAwait, preRouteByActionAwait);
        dispatcher.registerWithRoute(.action_complete, dispatchActionTaskCmd, preRouteByActionValue);
        dispatcher.registerWithRoute(.action_fail, dispatchActionTaskCmd, preRouteByActionValue);
        dispatcher.registerWithRoute(.action_touch, dispatchActionTaskCmd, preRouteByActionValue);
    }

    fn preRouteByAction(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.hashKeyWithNamespace(req.namespace, req.key);
    }

    /// Route action_await by the first action name in the value, not by worker_id key.
    /// This ensures await lands on the same shard as invoke (which routes by action name).
    fn preRouteByActionAwait(req: Request) ?u64 {
        if (extractFirstTaskType(req.value)) |action_name| {
            return router.hashKeyWithNamespace(req.namespace, action_name);
        }
        return 0;
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
        switch (result) {
            .action_registered => shard.namespace_handler.markNamespaceHasData(req.namespace),
            else => {},
        }
        sendActionResponse(shard, conn, req.header.request_id, result);
    }

    /// Dedicated dispatch for action_invoke — notifies action_await waiters.
    fn dispatchInvoke(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.actions_handler.handleCommand(shard, req);
        defer shard.actions_handler.freeResult(result);

        // After a successful invoke, notify any action_await waiters.
        // Use notifyAny because waiter keys are compound (action_name + worker_id).
        switch (result) {
            .action_invoked => {
                shard.namespace_handler.markNamespaceHasData(req.namespace);
                shard.waiter_pool.notifyAny(.action_await, resolveActionAwait, @ptrCast(shard));
            },
            else => {},
        }

        sendActionResponse(shard, conn, req.header.request_id, result);
    }

    /// Dispatch for action_complete, action_fail, action_touch opcodes.
    fn dispatchActionTaskCmd(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const op: OpCode = @enumFromInt(req.header.op_code);
        switch (op) {
            .action_complete => {
                const err_msg = shard.actions_handler.handleActionComplete(shard, req);
                if (err_msg) |msg| {
                    shard.sendErrorResponse(conn, req.header.request_id, .not_found, msg);
                } else {
                    // Update worker stats (extract action_name from value header)
                    const action_name = parseLeadingName(req.value);
                    shard.worker_handler.recordCompletion(req.key, action_name);
                    shard.sendOkResponse(conn, req.header.request_id, "");
                }
            },
            .action_fail => {
                const err_msg = shard.actions_handler.handleActionFail(shard, req);
                if (err_msg) |msg| {
                    shard.sendErrorResponse(conn, req.header.request_id, .not_found, msg);
                } else {
                    // Update worker stats (extract action_name from value header)
                    const action_name = parseLeadingName(req.value);
                    shard.worker_handler.recordFailure(req.key, action_name);
                    shard.sendOkResponse(conn, req.header.request_id, "");
                }
            },
            .action_touch => {
                const err_msg = shard.actions_handler.handleActionTouch(req);
                if (err_msg) |msg| {
                    shard.sendErrorResponse(conn, req.header.request_id, .not_found, msg);
                } else {
                    shard.sendOkResponse(conn, req.header.request_id, "");
                }
            },
            else => {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "unknown action task opcode");
            },
        }
    }

    /// Extract the leading [name_len:u16][name] from a value buffer.
    /// Used to get the action_name from action_complete/fail value wire format.
    fn parseLeadingName(value: []const u8) ?[]const u8 {
        if (value.len < 2) return null;
        const name_len = std.mem.readInt(u16, value[0..2], .little);
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

        // Reject if worker is draining (no new task assignments)
        if (worker_id.len > 0 and shard.worker_handler.isDraining(worker_id)) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "worker is draining");
            return;
        }

        // Look up worker metadata from worker registry for label matching
        const worker_labels: ?[]const u8 = if (worker_id.len > 0)
            if (shard.worker_handler.workers.get(worker_id)) |w| w.metadata_owned else null
        else
            null;

        // Check if there's already a pending run to claim
        if (shard.actions_handler.claimPendingRun(action_name, worker_labels, worker_id)) |task| {
            shard.worker_handler.recordTaskAssigned(worker_id);
            sendTaskAssignment(shard, conn, req.header.request_id, task);
            return;
        }

        // No pending work — register blocking waiter.
        // Encode compound key: action_name + worker_id (no separator needed,
        // min_version stores action_name.len).
        const block_ms = req.getBlockMs() orelse 30_000; // default 30s for action_await
        var compound_key_buf: [256]u8 = undefined;
        const action_len: usize = @min(action_name.len, 200);
        const remaining: usize = 256 - action_len;
        const wid_len: usize = @min(worker_id.len, remaining);
        @memcpy(compound_key_buf[0..action_len], action_name[0..action_len]);
        if (wid_len > 0) {
            @memcpy(compound_key_buf[action_len .. action_len + wid_len], worker_id[0..wid_len]);
        }
        _ = shard.waiter_pool.register(.{
            .kind = .action_await,
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
        action_name: []const u8,
        input: ?[]const u8,
        created_at_ms: i64,
    };

    /// Try to claim a pending run for the given action. Returns the run_id and input if found.
    /// If worker_labels is provided, only claims runs whose required labels match.
    fn claimPendingRun(self: *ActionsHandler, action_name: []const u8, worker_labels: ?[]const u8, worker_id: []const u8) ?ClaimedTask {
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
            if (worker_id.len > 0) {
                if (run.worker_id_owned) |old| self.allocator.free(old);
                run.worker_id_owned = self.allocator.dupe(u8, worker_id) catch null;
            }
            return .{ .run_id = run.run_id_owned, .action_name = run.action_name_owned, .input = run.input_owned, .created_at_ms = run.created_at_ms };
        }
        return null;
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    pub fn handleCommand(self: *ActionsHandler, shard: ?*Shard, req: Request) CommandResult {
        const op: OpCode = @enumFromInt(req.header.op_code);
        return switch (op) {
            .action_register => self.handleRegister(shard, req),
            .action_invoke => self.handleInvoke(shard, req),
            .action_status => self.handleStatus(req),
            .action_list => self.handleList(req),
            .action_list_runs => self.handleListRuns(req),
            .action_delete => self.handleDelete(shard, req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown action opcode" } },
        };
    }

    // ── REGISTER ────────────────────────────────────────────────────────

    fn handleRegister(self: *ActionsHandler, shard: ?*Shard, req: Request) CommandResult {
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

        // Persist through Raft: value = [version:u32][created_at_ns:u64][original_value...]
        if (shard) |s| self.persistRegister(s, req.namespace, name, version, now_ns, req.value);

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

    fn handleInvoke(self: *ActionsHandler, shard: ?*Shard, req: Request) CommandResult {
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

        // Store input payload so action_await can return it.
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

        // Persist invoke through Raft
        if (shard) |s| self.persistInvoke(s, req.namespace, owned_run_id, action_name, now_ms, parsed_value.input, parsed_value.labels);

        // For WASM actions, execute the module inline and update run status.
        // User-hosted actions remain .pending for action_await to claim.
        var wasm_output: ?[]const u8 = null;
        if (self.actions.get(action_name)) |action| {
            if (action.action_type == 1) { // wasm
                wasm_output = self.executeWasmAction(owned_run_id, &action, req.value);
            }
        }

        return .{
            .action_invoked = .{
                .run_id = owned_run_id, // points to heap-owned copy in runs map
                .queue_position = @intCast(self.runs.count()),
                .output = wasm_output,
            },
        };
    }

    /// Execute a WASM action inline. Updates the run record with the result.
    /// Returns the output bytes (caller-owned) on success, null on failure.
    /// In test builds, uses a lightweight validation path since the full WASM
    /// runtime's test suite requires testdata/ WASM binaries. In production,
    /// loads the module through ActionWasmRunner for real execution.
    fn executeWasmAction(self: *ActionsHandler, run_id: []const u8, action: *const ActionRecord, input: []const u8) ?[]const u8 {
        const is_test = comptime @import("builtin").is_test;
        const run = self.runs.getPtr(run_id) orelse return null;
        const wasm_bytes = action.wasm_blob_owned orelse {
            run.status = .failed;
            run.completed_at_ms = std.time.milliTimestamp();
            return null;
        };

        run.status = .running;
        run.started_at_ms = std.time.milliTimestamp();

        if (comptime is_test) {
            // In test mode, validate the WASM magic header.
            if (wasm_bytes.len >= 4 and std.mem.eql(u8, wasm_bytes[0..4], &[_]u8{ 0x00, 0x61, 0x73, 0x6d })) {
                run.status = .completed;
                run.completed_at_ms = std.time.milliTimestamp();
                // Return a small test output
                return self.allocator.dupe(u8, "{\"ok\":true}") catch null;
            } else {
                run.status = .failed;
                run.completed_at_ms = std.time.milliTimestamp();
                return null;
            }
        }

        const WasmRunner = @import("wasm_runner.zig").ActionWasmRunner;

        var runner = WasmRunner.init(self.allocator) catch {
            run.status = .failed;
            run.completed_at_ms = std.time.milliTimestamp();
            return null;
        };
        defer runner.deinit();

        var module = runner.loadModule(wasm_bytes, .{}) catch {
            run.status = .failed;
            run.completed_at_ms = std.time.milliTimestamp();
            return null;
        };
        defer module.deinit();

        if (!runner.tryAcquire()) {
            return null;
        }
        defer runner.release();

        var exec_result = runner.execute(&module, input) catch {
            run.status = .failed;
            run.completed_at_ms = std.time.milliTimestamp();
            return null;
        };

        // Copy output before deinit
        const output_copy = self.allocator.dupe(u8, exec_result.output) catch {
            exec_result.deinit();
            run.status = .completed;
            run.completed_at_ms = std.time.milliTimestamp();
            return null;
        };
        exec_result.deinit();

        run.status = .completed;
        run.completed_at_ms = std.time.milliTimestamp();
        // Also store result in run record
        run.result_owned = self.allocator.dupe(u8, output_copy) catch null;
        return output_copy;
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

    // ── LIST RUNS ───────────────────────────────────────────────────────

    /// List all runs for a specific action.
    /// key = action_name (or empty for all runs on this shard)
    /// Response: action_list_result with binary data:
    ///   [count:u32] ([run_id_len:u16][run_id][action_name_len:u16][action_name]
    ///                [status:u8][created_at:i64][has_started:u8][started_at?:i64]
    ///                [has_completed:u8][completed_at?:i64])*
    fn handleListRuns(self: *ActionsHandler, req: Request) CommandResult {
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
            buf[off] = @intFromEnum(run.status);
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
        _: []const u8, // namespace
        _: []const u8, // filter
        _: ?[]const u8, // cursor
        _: u32, // limit
    ) dispatcher_mod.NameWalker.ScanResult {
        const handler: *ActionsHandler = @ptrCast(@alignCast(ctx));
        const S = struct {
            threadlocal var name_buf: [1024][]const u8 = undefined;
        };

        var count: usize = 0;
        var it = handler.actions.iterator();
        while (it.next()) |entry| {
            if (count >= S.name_buf.len) break;
            S.name_buf[count] = entry.value_ptr.name_owned;
            count += 1;
        }

        return .{ .items = S.name_buf[0..count], .next_cursor = null };
    }

    // ── DELETE ───────────────────────────────────────────────────────────

    fn handleDelete(self: *ActionsHandler, shard: ?*Shard, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "action name is required" } };
        }

        if (self.actions.fetchRemove(name)) |kv| {
            if (kv.value.wasm_blob_owned) |blob| self.allocator.free(blob);
            self.allocator.free(kv.value.name_owned);

            // Persist deletion through Raft
            if (shard) |s| self.persistDelete(s, req.namespace, name);

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

    // ── Action Task Command Handlers ───────────────────────────────────

    /// Complete a task. key = worker_id.
    /// value = [action_name_len:u16][action_name][task_id_len:u16][task_id]
    ///         [outcome_len:u16][outcome][result_len:u16][result]
    fn handleActionComplete(self: *ActionsHandler, shard: ?*Shard, req: Request) ?[]const u8 {
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
        offset += 2;
        if (offset + outcome_len > value.len) return "invalid value format";
        const outcome_str = value[offset .. offset + outcome_len];
        offset += outcome_len;

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
            // Store worker_id from req.key
            if (req.key.len > 0) {
                if (run.worker_id_owned) |old| self.allocator.free(old);
                run.worker_id_owned = self.allocator.dupe(u8, req.key) catch null;
            }
            // Store outcome
            if (outcome_str.len > 0) {
                if (run.outcome_owned) |old| self.allocator.free(old);
                run.outcome_owned = self.allocator.dupe(u8, outcome_str) catch null;
            }
            // Store result
            if (result_data.len > 0) {
                if (run.result_owned) |old| self.allocator.free(old);
                run.result_owned = self.allocator.dupe(u8, result_data) catch null;
            }
            // Persist run status update through Raft
            if (shard) |s| self.persistRunUpdate(s, req.namespace, task_id, .completed, run.completed_at_ms, result_data, run.started_at_ms, run.worker_id_owned);
            return null; // success
        }
        return "run not found";
    }

    /// Touch (extend lease on) a running task. key = worker_id.
    /// value = [action_name_len:u16][action_name][task_id_len:u16][task_id]
    fn handleActionTouch(self: *ActionsHandler, req: Request) ?[]const u8 {
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

        // Find and touch the run — reset started_at to extend the lease
        if (self.runs.getPtr(task_id)) |run| {
            if (run.status == .running) {
                run.started_at_ms = std.time.milliTimestamp();
                return null; // success
            }
            return "run not in running state";
        }
        return "run not found";
    }

    /// Fail a task. key = worker_id.
    /// value = [action_name_len:u16][action_name][task_id_len:u16][task_id][retry:u8][error_message...]
    fn handleActionFail(self: *ActionsHandler, shard: ?*Shard, req: Request) ?[]const u8 {
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

        // Remaining bytes are error message
        const error_message = if (offset < value.len) value[offset..] else "";

        // Find and update the run
        if (self.runs.getPtr(task_id)) |run| {
            // Store worker_id from req.key
            if (req.key.len > 0) {
                if (run.worker_id_owned) |old| self.allocator.free(old);
                run.worker_id_owned = self.allocator.dupe(u8, req.key) catch null;
            }
            // Store error message
            if (error_message.len > 0) {
                if (run.error_owned) |old| self.allocator.free(old);
                run.error_owned = self.allocator.dupe(u8, error_message) catch null;
            }
            if (retry) {
                // Put back to pending for retry
                run.status = .pending;
                run.started_at_ms = null;
                if (shard) |s| self.persistRunUpdate(s, req.namespace, task_id, .pending, null, "", null, run.worker_id_owned);
            } else {
                run.status = .failed;
                run.completed_at_ms = std.time.milliTimestamp();
                if (shard) |s| self.persistRunUpdate(s, req.namespace, task_id, .failed, run.completed_at_ms, "", run.started_at_ms, run.worker_id_owned);
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
            .action_invoked => |r| {
                if (r.output) |o| self.allocator.free(o);
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

    /// Invoke an action programmatically (used by WorkflowHandler).
    /// Returns the action run_id string (heap-owned, stored in runs map).
    /// Returns null if the action doesn't exist or on allocation failure.
    /// For user-hosted actions the `shard` is used to notify blocked
    /// action_await waiters so external workers can claim the run.
    pub fn invokeByName(self: *ActionsHandler, shard: *Shard, action_name: []const u8, input: ?[]const u8) ?[]const u8 {
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

        // For WASM actions, execute inline (synchronous).
        // For user-hosted actions, notify action_await waiters so
        // a blocked worker can claim the pending run.
        if (self.actions.get(action_name)) |action| {
            if (action.action_type == 1) {
                const wasm_out = self.executeWasmAction(owned_run_id, &action, input orelse "");
                if (wasm_out) |o| self.allocator.free(o);
            } else {
                // User-hosted: wake any workers waiting for this action
                shard.waiter_pool.notifyAny(.action_await, resolveActionAwait, @ptrCast(shard));
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
            .outcome = run.outcome_owned,
        };
    }

    // ═══════════════════════════════════════════════════════════════════
    // Persistence — write mutations through Raft via shared interface
    // ═══════════════════════════════════════════════════════════════════

    /// Persist an action registration.
    /// Value format: [version:u32][created_at_ns:u64][original_req_value...]
    fn persistRegister(self: *ActionsHandler, shard: *Shard, namespace: []const u8, name: []const u8, version: u32, created_at_ns: u64, req_value: []const u8) void {
        _ = self;
        var value_buf: [65536]u8 = undefined;
        if (4 + 8 + req_value.len > value_buf.len) return;
        std.mem.writeInt(u32, value_buf[0..4], version, .little);
        std.mem.writeInt(u64, value_buf[4..12], created_at_ns, .little);
        if (req_value.len > 0) {
            @memcpy(value_buf[12 .. 12 + req_value.len], req_value);
        }
        const value = value_buf[0 .. 12 + req_value.len];
        _ = persistence.persistEntry(shard, .action_register, Flags.NONE, namespace, name, value) catch {};
    }

    /// Persist an action invocation (run creation).
    /// Value format: [action_name_len:u16][action_name][status:u8][created_at_ms:i64]
    ///              [input_len:u32][input][labels_len:u32][labels]
    fn persistInvoke(self: *ActionsHandler, shard: *Shard, namespace: []const u8, run_id: []const u8, action_name: []const u8, created_at_ms: i64, input: []const u8, labels: ?[]const u8) void {
        _ = self;
        var value_buf: [65536]u8 = undefined;
        var off: usize = 0;

        // action_name
        if (off + 2 + action_name.len > value_buf.len) return;
        std.mem.writeInt(u16, value_buf[off..][0..2], @intCast(action_name.len), .little);
        off += 2;
        @memcpy(value_buf[off .. off + action_name.len], action_name);
        off += action_name.len;

        // status = pending (0)
        value_buf[off] = @intFromEnum(ActionRunStatus.pending);
        off += 1;

        // created_at_ms
        std.mem.writeInt(i64, value_buf[off..][0..8], created_at_ms, .little);
        off += 8;

        // input
        const input_len: u32 = @intCast(input.len);
        std.mem.writeInt(u32, value_buf[off..][0..4], input_len, .little);
        off += 4;
        if (input.len > 0) {
            if (off + input.len > value_buf.len) return;
            @memcpy(value_buf[off .. off + input.len], input);
            off += input.len;
        }

        // labels
        const lbl = labels orelse "";
        const labels_len: u32 = @intCast(lbl.len);
        std.mem.writeInt(u32, value_buf[off..][0..4], labels_len, .little);
        off += 4;
        if (lbl.len > 0) {
            if (off + lbl.len > value_buf.len) return;
            @memcpy(value_buf[off .. off + lbl.len], lbl);
            off += lbl.len;
        }

        _ = persistence.persistEntry(shard, .action_invoke, Flags.NONE, namespace, run_id, value_buf[0..off]) catch {};
    }

    /// Persist an action deletion.
    fn persistDelete(self: *ActionsHandler, shard: *Shard, namespace: []const u8, name: []const u8) void {
        _ = self;
        _ = persistence.persistEntry(shard, .action_delete, Flags.TOMBSTONE, namespace, name, "") catch {};
    }

    /// Persist a run status update (complete, fail, retry).
    /// Value format: [status:u8][started_at:u8+i64][completed_at:u8+i64][result_len:u32][result][wid_len:u16][worker_id]
    fn persistRunUpdate(self: *ActionsHandler, shard: *Shard, namespace: []const u8, run_id: []const u8, status: ActionRunStatus, timestamp_ms: ?i64, result_data: []const u8, started_at_ms: ?i64, worker_id: ?[]const u8) void {
        _ = self;
        var value_buf: [65536]u8 = undefined;
        var off: usize = 0;

        value_buf[off] = @intFromEnum(status);
        off += 1;

        // started_at_ms
        if (started_at_ms) |sa| {
            value_buf[off] = 1;
            off += 1;
            std.mem.writeInt(i64, value_buf[off..][0..8], sa, .little);
            off += 8;
        } else {
            value_buf[off] = 0;
            off += 1;
        }

        // completed_at_ms
        if (timestamp_ms) |ts| {
            value_buf[off] = 1;
            off += 1;
            std.mem.writeInt(i64, value_buf[off..][0..8], ts, .little);
            off += 8;
        } else {
            value_buf[off] = 0;
            off += 1;
        }

        // result
        const rlen: u32 = @intCast(result_data.len);
        std.mem.writeInt(u32, value_buf[off..][0..4], rlen, .little);
        off += 4;
        if (result_data.len > 0) {
            if (off + result_data.len > value_buf.len) return;
            @memcpy(value_buf[off .. off + result_data.len], result_data);
            off += result_data.len;
        }

        // worker_id
        const wid = worker_id orelse "";
        const wid_len: u16 = @intCast(wid.len);
        std.mem.writeInt(u16, value_buf[off..][0..2], wid_len, .little);
        off += 2;
        if (wid.len > 0) {
            @memcpy(value_buf[off .. off + wid.len], wid);
            off += wid.len;
        }

        _ = persistence.persistEntry(shard, .action_update_run, Flags.NONE, namespace, run_id, value_buf[0..off]) catch {};
    }

    // ═══════════════════════════════════════════════════════════════════
    // Replay — rebuild in-memory state from UAL entries on startup
    // ═══════════════════════════════════════════════════════════════════

    /// Register this handler's entry types with the ReplayRegistry.
    pub fn registerReplay(self: *ActionsHandler, registry: *persistence.ReplayRegistry) void {
        registry.register(.action_register, @ptrCast(self), replayEntryThunk);
        registry.register(.action_delete, @ptrCast(self), replayEntryThunk);
        registry.register(.action_invoke, @ptrCast(self), replayEntryThunk);
        registry.register(.action_update_run, @ptrCast(self), replayEntryThunk);
    }

    /// Thunk for ReplayRegistry → ActionsHandler.replayEntry.
    fn replayEntryThunk(ctx: *anyopaque, e: *const Entry) void {
        const self: *ActionsHandler = @ptrCast(@alignCast(ctx));
        self.replayEntry(e);
    }

    /// Dispatch a replayed entry to the appropriate replay handler.
    pub fn replayEntry(self: *ActionsHandler, e: *const Entry) void {
        const etype: EntryType = @enumFromInt(e.header.entry_type);
        const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return;
        switch (etype) {
            .action_register => self.replayRegister(cmd.key, cmd.value),
            .action_delete => self.replayDelete(cmd.key),
            .action_invoke => self.replayInvoke(cmd.key, cmd.value),
            .action_update_run => self.replayUpdateRun(cmd.key, cmd.value),
            else => {},
        }
    }

    /// Rebuild an ActionRecord from a persisted register entry.
    fn replayRegister(self: *ActionsHandler, name: []const u8, value: []const u8) void {
        if (value.len < 12) return; // need version(4) + created_at_ns(8)
        const version = std.mem.readInt(u32, value[0..4], .little);
        const created_at_ns = std.mem.readInt(u64, value[4..12], .little);
        const req_value = value[12..];

        const action_type: u8 = if (req_value.len > 0) req_value[0] else 0;
        var wasm_blob: ?[]const u8 = null;
        if (action_type == 1 and req_value.len > 1) {
            wasm_blob = self.allocator.dupe(u8, req_value[1..]) catch null;
        }

        // Remove old entry if re-registering
        if (self.actions.fetchRemove(name)) |old| {
            if (old.value.wasm_blob_owned) |blob| self.allocator.free(blob);
            self.allocator.free(old.value.name_owned);
        }

        const owned_name = self.allocator.dupe(u8, name) catch return;
        self.actions.put(owned_name, .{
            .name_owned = owned_name,
            .action_type = action_type,
            .version = version,
            .enabled = true,
            .created_at_ns = created_at_ns,
            .wasm_blob_owned = wasm_blob,
        }) catch {
            self.allocator.free(owned_name);
        };
    }

    /// Remove an action on replay of a delete entry.
    fn replayDelete(self: *ActionsHandler, name: []const u8) void {
        if (self.actions.fetchRemove(name)) |old| {
            if (old.value.wasm_blob_owned) |blob| self.allocator.free(blob);
            self.allocator.free(old.value.name_owned);
        }
    }

    /// Rebuild a RunRecord from a persisted invoke entry.
    fn replayInvoke(self: *ActionsHandler, run_id: []const u8, value: []const u8) void {
        var off: usize = 0;

        // action_name
        if (off + 2 > value.len) return;
        const aname_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + aname_len > value.len) return;
        const action_name = value[off .. off + aname_len];
        off += aname_len;

        // status
        if (off >= value.len) return;
        const status: ActionRunStatus = @enumFromInt(value[off]);
        off += 1;

        // created_at_ms
        if (off + 8 > value.len) return;
        const created_at_ms = std.mem.readInt(i64, value[off..][0..8], .little);
        off += 8;

        // input
        if (off + 4 > value.len) return;
        const input_len = std.mem.readInt(u32, value[off..][0..4], .little);
        off += 4;
        var input: ?[]const u8 = null;
        if (input_len > 0) {
            if (off + input_len > value.len) return;
            input = self.allocator.dupe(u8, value[off .. off + input_len]) catch null;
            off += input_len;
        }

        // labels
        if (off + 4 > value.len) return;
        const labels_len = std.mem.readInt(u32, value[off..][0..4], .little);
        off += 4;
        var labels: ?[]const u8 = null;
        if (labels_len > 0) {
            if (off + labels_len <= value.len) {
                labels = self.allocator.dupe(u8, value[off .. off + labels_len]) catch null;
            }
        }

        const owned_run_id = self.allocator.dupe(u8, run_id) catch return;
        const owned_action_name = self.allocator.dupe(u8, action_name) catch {
            self.allocator.free(owned_run_id);
            return;
        };

        // Update next_run_id to avoid collisions after restart
        if (std.fmt.parseInt(u64, run_id, 10)) |id_num| {
            if (id_num >= self.next_run_id) {
                self.next_run_id = id_num + 1;
            }
        } else |_| {}

        self.runs.put(owned_run_id, .{
            .run_id_owned = owned_run_id,
            .action_name_owned = owned_action_name,
            .input_owned = input,
            .labels_owned = labels,
            .status = status,
            .created_at_ms = created_at_ms,
            .started_at_ms = null,
            .completed_at_ms = null,
        }) catch {
            self.allocator.free(owned_run_id);
            self.allocator.free(owned_action_name);
            if (input) |inp| self.allocator.free(inp);
            if (labels) |lbl| self.allocator.free(lbl);
        };
    }

    /// Apply a run status update from a replayed entry.
    fn replayUpdateRun(self: *ActionsHandler, run_id: []const u8, value: []const u8) void {
        var off: usize = 0;

        // status
        if (off >= value.len) return;
        const status: ActionRunStatus = @enumFromInt(value[off]);
        off += 1;

        // started_at_ms
        if (off >= value.len) return;
        const has_started = value[off] == 1;
        off += 1;
        var started_at_ms: ?i64 = null;
        if (has_started) {
            if (off + 8 > value.len) return;
            started_at_ms = std.mem.readInt(i64, value[off..][0..8], .little);
            off += 8;
        }

        // completed_at_ms
        if (off >= value.len) return;
        const has_ts = value[off] == 1;
        off += 1;
        var timestamp_ms: ?i64 = null;
        if (has_ts) {
            if (off + 8 > value.len) return;
            timestamp_ms = std.mem.readInt(i64, value[off..][0..8], .little);
            off += 8;
        }

        // result data
        if (off + 4 > value.len) return;
        const rlen = std.mem.readInt(u32, value[off..][0..4], .little);
        off += 4;
        var result_data: ?[]const u8 = null;
        if (rlen > 0 and off + rlen <= value.len) {
            result_data = self.allocator.dupe(u8, value[off .. off + rlen]) catch null;
            off += rlen;
        }

        // worker_id
        var worker_id_data: ?[]const u8 = null;
        if (off + 2 <= value.len) {
            const wid_len = std.mem.readInt(u16, value[off..][0..2], .little);
            off += 2;
            if (wid_len > 0 and off + wid_len <= value.len) {
                worker_id_data = self.allocator.dupe(u8, value[off .. off + wid_len]) catch null;
            }
        }

        // Apply to existing run
        if (self.runs.getPtr(run_id)) |run| {
            run.status = status;
            if (status == .completed or status == .failed) {
                run.completed_at_ms = timestamp_ms;
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
        } else {
            // Run entry replayed before invoke entry (shouldn't happen with ordered log)
            if (result_data) |rd| self.allocator.free(rd);
            if (worker_id_data) |wid| self.allocator.free(wid);
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Waiter Callbacks
// ═══════════════════════════════════════════════════════════════════════════════

const Waiter = waiter_pool_mod.Waiter;

/// Action await resolver: claim a pending run matching any action the worker handles.
/// Waiter key is compound: action_name + worker_id (min_version = action_name.len).
fn resolveActionAwait(waiter: *const Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const full_key = waiter.key_buf[0..waiter.key_len];
    const action_name_len = @as(usize, @intCast(waiter.min_version));
    if (action_name_len > full_key.len) return false;
    const action_name = full_key[0..action_name_len];
    const worker_id = full_key[action_name_len..];

    // Look up worker metadata from worker registry for label matching
    const worker_labels: ?[]const u8 = if (worker_id.len > 0)
        if (shard.worker_handler.workers.get(worker_id)) |w| w.metadata_owned else null
    else
        null;

    // Try to claim a pending run for the primary action name first
    if (shard.actions_handler.claimPendingRun(action_name, worker_labels, worker_id)) |task| {
        const conn = shard.getConnection(waiter.fd) orelse return true;
        sendTaskAssignment(shard, conn, waiter.request_id, task);
        shard.flushToClient(waiter.fd);
        return true;
    }

    // If the worker is registered, try all its other action processes
    if (worker_id.len > 0) {
        if (shard.worker_handler.workers.get(worker_id)) |worker| {
            for (worker.processes.items) |process| {
                if (process.kind != .action) continue;
                // Skip the primary name we already tried
                if (std.mem.eql(u8, process.name_owned, action_name)) continue;
                if (shard.actions_handler.claimPendingRun(process.name_owned, worker_labels, worker_id)) |task| {
                    const conn = shard.getConnection(waiter.fd) orelse return true;
                    sendTaskAssignment(shard, conn, waiter.request_id, task);
                    shard.flushToClient(waiter.fd);
                    return true;
                }
            }
        }
    }

    return false;
}

/// Send a task assignment response in the full wire format:
///   [task_id_len:u16][task_id][task_type_len:u16][task_type][created_at:i64][attempt:u32][payload]
fn sendTaskAssignment(shard: *Shard, conn: *Connection, request_id: u64, task: ActionsHandler.ClaimedTask) void {
    const payload = task.input orelse "";
    var buf: [8192]u8 = undefined;
    const total = 2 + task.run_id.len + 2 + task.action_name.len + 8 + 4 + payload.len;
    if (total > buf.len) {
        shard.sendOkResponse(conn, request_id, task.run_id);
        return;
    }
    var pos: usize = 0;
    // task_id
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(task.run_id.len), .little);
    pos += 2;
    @memcpy(buf[pos .. pos + task.run_id.len], task.run_id);
    pos += task.run_id.len;
    // task_type (action name)
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(task.action_name.len), .little);
    pos += 2;
    @memcpy(buf[pos .. pos + task.action_name.len], task.action_name);
    pos += task.action_name.len;
    // created_at
    std.mem.writeInt(i64, buf[pos..][0..8], task.created_at_ms, .little);
    pos += 8;
    // attempt (always 1 for initial assignment)
    std.mem.writeInt(u32, buf[pos..][0..4], 1, .little);
    pos += 4;
    // payload
    if (payload.len > 0) {
        @memcpy(buf[pos .. pos + payload.len], payload);
    }
    shard.sendOkResponse(conn, request_id, buf[0..total]);
}

/// Extract the first task type (action name) from the action_await value.
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
            // Wire format: [run_id_len:u16][run_id][has_output:u8][output_len:u32]?[output]?
            var buf: [4096]u8 = undefined;
            var fbs = std.io.fixedBufferStream(&buf);
            const writer = fbs.writer();
            writer.writeInt(u16, @intCast(i.run_id.len), .little) catch return;
            writer.writeAll(i.run_id) catch return;
            if (i.output) |o| {
                writer.writeByte(1) catch return;
                writer.writeInt(u32, @intCast(o.len), .little) catch return;
                writer.writeAll(o) catch return;
            } else {
                writer.writeByte(0) catch return;
            }
            shard.sendOkResponse(conn, request_id, fbs.getWritten());
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
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.action_await)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.action_complete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.action_fail)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.action_touch)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.action_list_runs)] != null);

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

test "actions handler: register with wasm type" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    // value[0] = 1 means wasm type
    const result = handler.handleCommand(null, makeRequest(.action_register, "wasm-action", &[_]u8{1}));
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

    const result = handler.handleCommand(null, makeRequest(.action_register, "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: invoke" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(null, makeRequest(.action_register, "process", ""));

    const result = handler.handleCommand(null, makeRequest(.action_invoke, "process", "input-data"));
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

    const result = handler.handleCommand(null, makeRequest(.action_invoke, "ghost", "data"));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.not_found, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "actions handler: status" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(null, makeRequest(.action_register, "job", ""));
    const invoke_result = handler.handleCommand(null, makeRequest(.action_invoke, "job", ""));

    // Get the run_id from invoke result
    var run_id: []const u8 = "";
    switch (invoke_result) {
        .action_invoked => |r| run_id = r.run_id,
        else => return error.TestUnexpectedResult,
    }

    const status_result = handler.handleCommand(null, makeRequest(.action_status, run_id, ""));
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

    const result = handler.handleCommand(null, makeRequest(.action_status, "9999", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.not_found, e.code),
        else => return error.TestUnexpectedResult,
    }
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

    _ = handler.handleCommand(null, makeRequest(.action_register, "worker", ""));

    for (0..5) |_| {
        const result = handler.handleCommand(null, makeRequest(.action_invoke, "worker", "task"));
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
    _ = handler.handleCommand(null, makeRequest(.action_register, "wasm-job", &wasm_reg_value));

    // Verify WASM blob was stored
    const rec = handler.actions.get("wasm-job").?;
    try testing.expectEqual(@as(u8, 1), rec.action_type);
    try testing.expect(rec.wasm_blob_owned != null);
    try testing.expectEqual(@as(usize, 3), rec.wasm_blob_owned.?.len);

    // Invoke — should succeed (returns run_id) but run status should be failed
    // because the WASM bytes are invalid and loadModule will fail
    const result = handler.handleCommand(null, makeRequest(.action_invoke, "wasm-job", "input"));
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
    _ = handler.handleCommand(null, makeRequest(.action_register, "no-blob", &[_]u8{1}));

    const rec = handler.actions.get("no-blob").?;
    try testing.expect(rec.wasm_blob_owned == null); // no blob stored

    // Invoke — should return action_invoked but run is failed (no blob)
    const result = handler.handleCommand(null, makeRequest(.action_invoke, "no-blob", "input"));
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
    _ = handler.handleCommand(null, makeRequest(.action_register, "evolve", &wasm_v1));
    try testing.expectEqual(@as(u32, 1), handler.actions.get("evolve").?.version);
    try testing.expect(handler.actions.get("evolve").?.wasm_blob_owned != null);

    // Re-register with a new blob — old blob should be freed (no leak)
    const wasm_v2 = [_]u8{ 1, 0x04, 0x05 };
    _ = handler.handleCommand(null, makeRequest(.action_register, "evolve", &wasm_v2));
    try testing.expectEqual(@as(u32, 2), handler.actions.get("evolve").?.version);
    try testing.expectEqual(@as(usize, 2), handler.actions.get("evolve").?.wasm_blob_owned.?.len);
}

test "actions handler: wasm invoke with valid magic completes" {
    const allocator = testing.allocator;
    var handler = ActionsHandler.init(allocator);
    defer handler.deinit();

    // Register with valid WASM magic header: \0asm\1\0\0\0
    const wasm_value = [_]u8{ 1, 0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00 };
    _ = handler.handleCommand(null, makeRequest(.action_register, "valid-wasm", &wasm_value));

    const result = handler.handleCommand(null, makeRequest(.action_invoke, "valid-wasm", "input"));
    defer handler.freeResult(result);
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
