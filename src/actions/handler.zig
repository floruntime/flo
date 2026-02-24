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
    };

    pub const RunRecord = struct {
        run_id_owned: []const u8,
        action_name_owned: []const u8,
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
        // Free all owned action names
        var ait = self.actions.iterator();
        while (ait.next()) |entry| {
            self.allocator.free(entry.value_ptr.name_owned);
        }
        self.actions.deinit();

        // Free all owned run data
        var rit = self.runs.iterator();
        while (rit.next()) |entry| {
            self.allocator.free(entry.value_ptr.run_id_owned);
            self.allocator.free(entry.value_ptr.action_name_owned);
        }
        self.runs.deinit();
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.action_register, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.action_invoke, dispatchInvoke, preRouteByAction);
        dispatcher.registerWithRoute(.action_status, dispatchAction, preRouteByAction);
        dispatcher.register(.action_list, dispatchAction);
        dispatcher.registerWithRoute(.action_delete, dispatchAction, preRouteByAction);
        dispatcher.registerWithRoute(.worker_await, dispatchWorkerAwait, preRouteByAction);
    }

    fn preRouteByAction(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return std.hash.Wyhash.hash(0, req.key);
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

        // After a successful invoke, notify any worker_await waiters for this action
        switch (result) {
            .action_invoked => {
                if (req.key.len > 0) {
                    shard.waiter_pool.notify(.worker_await, req.key, resolveWorkerAwait, @ptrCast(shard));
                }
            },
            else => {},
        }

        sendActionResponse(shard, conn, req.header.request_id, result);
    }

    /// Blocking wait for a task to be dispatched.  Workers call this to receive work.
    fn dispatchWorkerAwait(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        if (req.key.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "action name is required");
            return;
        }

        // Check if there's already a pending run to claim
        if (shard.actions_handler.claimPendingRun(req.key)) |run_id| {
            shard.sendOkResponse(conn, req.header.request_id, run_id);
            return;
        }

        // No pending work — register blocking waiter
        const block_ms = req.getBlockMs() orelse 30_000; // default 30s for worker_await
        _ = shard.waiter_pool.register(.{
            .kind = .worker_await,
            .fd = conn.fd,
            .request_id = req.header.request_id,
            .key = req.key,
            .min_version = 0,
            .timeout_ms = block_ms,
        });
        conn.response_deferred = true;
    }

    /// Try to claim a pending run for the given action. Returns the run_id if found.
    fn claimPendingRun(self: *ActionsHandler, action_name: []const u8) ?[]const u8 {
        var it = self.runs.iterator();
        while (it.next()) |entry| {
            const run = entry.value_ptr;
            if (run.status == .pending and std.mem.eql(u8, run.action_name_owned, action_name)) {
                run.status = .running;
                run.started_at_ms = std.time.milliTimestamp();
                return run.run_id_owned;
            }
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
            self.allocator.free(old.value.name_owned);
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
        }) catch {
            self.allocator.free(owned_name);
            return .{ .err = .{ .code = .internal_error, .message = "action store failed" } };
        };

        // Version string
        var ver_buf: [12]u8 = undefined;
        const ver_str = std.fmt.bufPrint(&ver_buf, "{d}", .{version}) catch "1";

        return .{ .action_registered = .{
            .name = name, // req-owned, same lifetime as request
            .version = ver_str,
        } };
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

        const now_ms: i64 = std.time.milliTimestamp();

        self.runs.put(owned_run_id, .{
            .run_id_owned = owned_run_id,
            .action_name_owned = owned_action_name,
            .status = .pending,
            .created_at_ms = now_ms,
            .started_at_ms = null,
            .completed_at_ms = null,
        }) catch {
            self.allocator.free(owned_run_id);
            self.allocator.free(owned_action_name);
            return .{ .err = .{ .code = .internal_error, .message = "run store failed" } };
        };

        return .{ .action_invoked = .{
            .run_id = owned_run_id, // points to heap-owned copy in runs map
            .queue_position = @intCast(self.runs.count()),
        } };
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

    // ── Serialization ───────────────────────────────────────────────────

    /// Wire format: [count:u32] ([name_len:u16][name][type:u8][version:u32][enabled:u8])*
    fn serializeActionList(self: *ActionsHandler) ![]u8 {
        var total_size: usize = 4;
        var entry_count: u32 = 0;
        var it = self.actions.iterator();
        while (it.next()) |entry| {
            total_size += 2 + entry.value_ptr.name_owned.len + 1 + 4 + 1; // name_len + name + type + version + enabled
            entry_count += 1;
        }

        const buf = try self.allocator.alloc(u8, total_size);
        errdefer self.allocator.free(buf);
        var offset: usize = 0;

        std.mem.writeInt(u32, buf[offset..][0..4], entry_count, .little);
        offset += 4;

        var it2 = self.actions.iterator();
        while (it2.next()) |entry| {
            const rec = entry.value_ptr;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(rec.name_owned.len), .little);
            offset += 2;
            @memcpy(buf[offset .. offset + rec.name_owned.len], rec.name_owned);
            offset += rec.name_owned.len;
            buf[offset] = rec.action_type;
            offset += 1;
            std.mem.writeInt(u32, buf[offset..][0..4], rec.version, .little);
            offset += 4;
            buf[offset] = if (rec.enabled) 1 else 0;
            offset += 1;
        }

        return buf;
    }

    // ── Free Result ─────────────────────────────────────────────────────

    pub fn freeResult(self: *ActionsHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .action_list_result => |r| self.allocator.free(r.data),
            else => {},
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Waiter Callbacks
// ═══════════════════════════════════════════════════════════════════════════════

const Waiter = waiter_pool_mod.Waiter;

/// Worker await resolver: claim a pending run matching the action name.
fn resolveWorkerAwait(waiter: *const Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const action_name = waiter.key();

    // Try to claim a pending run
    const run_id = shard.actions_handler.claimPendingRun(action_name) orelse return false;

    const conn = shard.getConnection(waiter.fd) orelse return true; // connection gone
    shard.sendOkResponse(conn, waiter.request_id, run_id);
    return true;
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

    try testing.expectEqual(@as(u16, 6), dispatcher.handler_count);
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
