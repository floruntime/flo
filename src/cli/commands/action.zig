//! Action & Worker commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo action register <name> [--owner <owner>] [--timeout <ms>]
//!   flo action invoke <name> <input> [--labels <json>]
//!   flo action status <run_id>
//!   flo action list [--limit <n>]
//!   flo action delete <name>

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const outcome = @import("../outcome.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const cli_output = @import("../output.zig");
const wire = @import("../../util/wire.zig");
const WireReader = wire.WireReader;
const cli_config = @import("../config.zig");
const schema = @import("../../protocol/schema.zig");
const action_ops = @import("../../protocol/action_ops.zig");
const worker_ops = @import("../../protocol/worker_ops.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the action command tree
pub fn createActionCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("action")
        .about("Action & workflow operations")
        .group("Data Commands")
        .longAbout(
            \\Manage serverless actions and workflows.
            \\
            \\Actions are functions that can be invoked on-demand with automatic
            \\scaling, retries, and execution tracking.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("register")
                .about("Register a new action")
                .examples(&.{
                    "flo action register myaction",
                    "flo action register process --owner myteam --retries 3",
                })
                .arg("name", "Action name")
                .stringFlag("owner", 0, "cli", "Owner/team name")
                .uintFlag("timeout", 0, 30000, "Execution timeout (ms)")
                .uintFlag("retries", 'r', 0, "Max retry attempts")
                .action(wrapHandler(runRegister)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("invoke")
                .about("Invoke an action")
                .aliases(&.{"call"})
                .examples(&.{
                    "flo action invoke myaction '{\"key\":\"value\"}'",
                    "flo action invoke render '{\"frame\":1}' --labels '{\"gpu\":true}'",
                })
                .arg("name", "Action name")
                .arg("input", "Input payload (JSON)")
                .stringFlag("labels", 'l', "", "Required worker labels (JSON, e.g. '{\"gpu\":true}')")
                .action(wrapHandler(runInvoke)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("status")
                .about("Get execution status")
                .arg("run_id", "Run ID to check")
                .action(wrapHandler(runStatus)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List registered actions")
                .aliases(&.{"ls"})
                .uintFlag("limit", 'l', 100, "Maximum actions to show")
                .stringFlag("prefix", 0, "", "Only actions whose names start with this")
                .action(wrapHandler(runList)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("delete")
                .about("Delete an action")
                .aliases(&.{"rm"})
                .arg("name", "Action name to delete")
                .action(wrapHandler(runDelete)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("runs")
                .about("List action runs/tasks")
                .aliases(&.{"tasks"})
                .arg("name", "Action name")
                .uintFlag("limit", 'l', 50, "Maximum runs to show")
                .action(wrapHandler(runRuns)),
        )
        .build();
}

/// Create the worker command tree
pub fn createWorkerCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("worker")
        .about("Worker management commands")
        .group("Data Commands")
        .longAbout(
            \\Manage task workers for distributed processing.
            \\
            \\Workers pull tasks from task queues, execute them, and report results.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("register")
                .about("Register a worker")
                .examples(&.{
                    "flo worker register worker-1 task_type_a task_type_b",
                    "flo worker register gpu-worker-1 render --labels '{\"gpu\":true,\"vram_gb\":24}'",
                })
                .arg("worker_id", "Worker identifier")
                .variadicArg("task_types", "Task types to handle, one or more")
                .stringFlag("labels", 'l', "", "Worker labels (JSON, e.g. '{\"gpu\":true}')")
                .action(wrapHandler(runWorkerRegister)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("await")
                .about("Wait for tasks")
                .variadicArg("task_types", "Task types to wait for, one or more")
                .stringFlag("worker-id", 'w', "", "Worker ID (required)")
                .uintFlag("block", 'b', 5000, "Block for tasks (ms, at most 300000; 0 = don't wait)")
                .action(wrapHandler(runWorkerAwait)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("complete")
                .about("Complete a task successfully")
                .arg("task_id", "Task ID, as await printed it")
                .stringFlag("worker-id", 'w', "", "Worker ID (required)")
                .stringFlag("result", 0, "", "Result payload (JSON)")
                .stringFlag("outcome", 0, "", "The outcome a workflow step follows (default success)")
                .action(wrapHandler(runWorkerComplete)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("fail")
                .about("Fail a task")
                .arg("task_id", "Task ID, as await printed it")
                .stringFlag("worker-id", 'w', "", "Worker ID (required)")
                .stringFlag("error", 0, "", "Error message")
                .boolFlag("retry", 'r', "Allow retry")
                .action(wrapHandler(runWorkerFail)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("touch")
                .about("Keep a task: its lease runs a full lease from now")
                .arg("task_id", "Task ID, as await printed it")
                .stringFlag("worker-id", 'w', "", "Worker ID (required)")
                .action(wrapHandler(runWorkerTouch)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List registered workers")
                .aliases(&.{"ls"})
                .uintFlag("limit", 'l', 0, "Most workers to show (0: all)")
                .action(wrapHandler(runWorkerList)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("drain")
                .about("Drain a worker (stop new task assignments)")
                .examples(&.{
                    "flo worker drain worker-1",
                    "flo worker drain worker-1 -n my-namespace",
                })
                .arg("worker_id", "Worker identifier to drain")
                .action(wrapHandler(runWorkerDrain)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("info")
                .about("Show worker details")
                .examples(&.{
                    "flo worker info worker-1",
                })
                .arg("worker_id", "Worker identifier")
                .action(wrapHandler(runWorkerInfo)),
        )
        .build();
}

fn runRegister(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?; // validated by commander

    const owner = ctx.getString("owner") orelse "cli";
    const timeout = ctx.getUint("timeout") orelse 30000;
    const retries = ctx.getUint("retries") orelse 0;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // register(client, namespace, action_name, action_type, owner, timeout_ms, max_retries, retry_delay_ms, wasm_bytes)
    var result = client_mod.action.register(&client, namespace, name, 0, owner, @intCast(timeout), @intCast(retries), null, null) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("Registered action: {s}\n", .{name});
}

fn runInvoke(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?; // validated by commander
    const input = ctx.getPositional("input").?; // validated by commander

    const labels_str = ctx.getString("labels") orelse "";
    const required_labels: ?[]const u8 = if (labels_str.len > 0) labels_str else null;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.action.invoke(&client, namespace, name, input, required_labels) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    // Response wire format: [run_id_len:u16][run_id][has_output:u8][output_len:u32]?[output]?
    if (result.asRawData()) |data| {
        if (data.len >= 2) {
            const run_id_len = std.mem.readInt(u16, data[0..2], .little);
            const run_id_end = @as(usize, 2) + run_id_len;
            if (data.len >= run_id_end and run_id_len > 0) {
                const run_id = data[2..run_id_end];
                ctx.print("Result: {s}\n", .{run_id});

                // Check for output after run_id
                if (data.len > run_id_end and data[run_id_end] == 1) {
                    const out_off = run_id_end + 1;
                    if (data.len >= out_off + 4) {
                        const out_len = std.mem.readInt(u32, data[out_off..][0..4], .little);
                        if (data.len >= out_off + 4 + out_len) {
                            ctx.print("Output: {s}\n", .{data[out_off + 4 ..][0..out_len]});
                        }
                    }
                }
                return;
            }
        }
    }
    ctx.print("OK\n", .{});
}

fn runStatus(ctx: *commander.Context) commander.Error!void {
    const run_id = ctx.getPositional("run_id").?; // validated by commander

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // status(client, namespace, run_id)
    var result = client_mod.action.status(&client, namespace, run_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        return outcome.notFound(ctx, "Run not found: {s}", .{run_id});
    }

    try outcome.check(ctx, result);

    var diag: schema.Diagnostic = .{};
    const s = action_ops.op("action.status").response.decodeAll(result.data, &diag) catch return outcome.malformed(ctx, "action.status answer");
    if (cli_output.getFormat(ctx) == .json) {
        ctx.print("{{\"run_id\":{f},\"status\":\"{s}\",\"created_at_ms\":{d},\"started_at_ms\":", .{ cli_output.jsonString(run_id), @tagName(s.status), s.created_at_ms });
        if (s.started_at_ms) |v| ctx.print("{d}", .{v}) else ctx.print("null", .{});
        ctx.print(",\"completed_at_ms\":", .{});
        if (s.completed_at_ms) |v| ctx.print("{d}", .{v}) else ctx.print("null", .{});
        ctx.print(",\"attempt\":{d},\"output\":", .{s.attempt});
        if (s.output) |v| ctx.print("{f}", .{cli_output.jsonString(v)}) else ctx.print("null", .{});
        ctx.print(",\"error\":", .{});
        if (s.error_message) |v| ctx.print("{f}", .{cli_output.jsonString(v)}) else ctx.print("null", .{});
        ctx.print(",\"worker_id\":", .{});
        if (s.worker_id) |v| ctx.print("{f}", .{cli_output.jsonString(v)}) else ctx.print("null", .{});
        ctx.print("}}\n", .{});
        return;
    }
    ctx.print("Run: {s}\nstatus: {s}\ncreated_at: {d}\n", .{ run_id, @tagName(s.status), s.created_at_ms });
    if (s.started_at_ms) |v| ctx.print("started_at: {d}\n", .{v});
    if (s.completed_at_ms) |v| ctx.print("completed_at: {d}\n", .{v});
    ctx.print("attempt: {d}\n", .{s.attempt});
    if (s.worker_id) |v| ctx.print("worker_id: {s}\n", .{v});
    if (s.output) |v| ctx.print("output: {s}\n", .{v});
    if (s.error_message) |v| ctx.print("error: {s}\n", .{v});
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const limit = ctx.getUint("limit") orelse 100;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    const prefix: ?[]const u8 = if (ctx.getString("prefix")) |p| if (p.len > 0) p else null else null;

    // Collect all action names across shards using cursor-based shard walking
    var all_names: std.ArrayList([]const u8) = .empty;
    defer {
        for (all_names.items) |name| {
            ctx.allocator.free(name);
        }
        all_names.deinit(ctx.allocator);
    }

    var cursor: ?[]const u8 = null;
    var cursor_owned: ?[]u8 = null;
    defer if (cursor_owned) |c| ctx.allocator.free(c);

    // Walk all shards until no more data
    while (all_names.items.len < limit) {
        var result = client_mod.action.list(&client, namespace, prefix, cursor) catch |err| return outcome.requestFailed(ctx, err);
        defer result.deinit();

        try outcome.check(ctx, result);

        // action_list returns scan format:
        // [count:u32] ([key_len:u16][key][value_len:u32][value])* [has_more:u8] [cursor_len:u16][cursor]?
        const what = "action list";
        var reader = WireReader.init(result.data);
        const count = reader.readU32() orelse return outcome.malformed(ctx, what);

        // Read the whole page, so its trailer is read from the right place
        // even when the limit is reached partway.
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            const key_len = reader.readU16() orelse return outcome.malformed(ctx, what);
            const key = reader.readSlice(key_len) orelse return outcome.malformed(ctx, what);
            // keys_only, but the value field is still present
            const val_len = reader.readU32() orelse return outcome.malformed(ctx, what);
            _ = reader.readSlice(val_len) orelse return outcome.malformed(ctx, what);
            if (all_names.items.len >= limit) continue;

            // Extract action name from key (format: _action:{name})
            const name = if (std.mem.startsWith(u8, key, "_action:"))
                key["_action:".len..]
            else
                key;

            const name_copy = try ctx.allocator.dupe(u8, name);
            all_names.append(ctx.allocator, name_copy) catch |err| {
                ctx.allocator.free(name_copy);
                return err;
            };
        }

        const has_more = (reader.readU8() orelse return outcome.malformed(ctx, what)) != 0;
        const cursor_len = reader.readU16() orelse return outcome.malformed(ctx, what);
        const next_cursor = if (cursor_len > 0) reader.readSlice(cursor_len) orelse return outcome.malformed(ctx, what) else null;

        // Free previous cursor and copy new one
        if (cursor_owned) |c| ctx.allocator.free(c);
        cursor_owned = null;

        if (!has_more or next_cursor == null) break;

        // Copy cursor for next iteration
        cursor_owned = try ctx.allocator.dupe(u8, next_cursor.?);
        cursor = cursor_owned;
    }

    // Output results
    if (cli_output.getFormat(ctx) == .json) {
        // An object per action, as `ns ls` gives a namespace.
        ctx.print("[", .{});
        for (all_names.items, 0..) |name, i| ctx.print("{s}{{\"name\":{f}}}", .{ if (i == 0) "" else ",", cli_output.jsonString(name) });
        ctx.print("]\n", .{});
    } else if (all_names.items.len == 0) {
        ctx.print("(no actions)\n", .{});
    } else {
        for (all_names.items) |name| {
            ctx.print("{s}\n", .{name});
        }
    }
}

fn runDelete(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?; // validated by commander

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // delete(client, namespace, action_name)
    var result = client_mod.action.delete(&client, namespace, name) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("Deleted action: {s}\n", .{name});
}

fn runRuns(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?; // validated by commander
    const namespace = cli_config.getNamespace(ctx);
    const limit: u32 = @intCast(ctx.getUint("limit") orelse 50);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.action.listRuns(&client, namespace, name, limit) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    const data = result.asRawData() orelse {
        cli_output.printEmptyList(ctx, "(no runs)");
        return;
    };

    // Server wire format is read directly — enum_u8 maps status byte to string,
    // timestamp_i64 formats as relative time, optional_timestamp_i64 handles has:u8 + i64.
    cli_output.printWireList(ctx, data, "(no runs)", &.{
        .{ .field = "run_id", .header = "RUN ID", .field_type = .str_u16 },
        .{ .field = "action", .header = "ACTION", .field_type = .str_u16 },
        .{ .field = "status", .header = "STATUS", .field_type = .enum_u8, .enum_labels = &.{ "pending", "running", "completed", "failed", "cancelled", "timed_out" } },
        .{ .field = "created", .header = "CREATED", .field_type = .timestamp_i64 },
        .{ .field = "started", .header = "STARTED", .field_type = .optional_timestamp_i64 },
        .{ .field = "completed", .header = "COMPLETED", .field_type = .optional_timestamp_i64 },
    }) catch return outcome.malformed(ctx, "list");
}

// Worker handlers

fn runWorkerRegister(ctx: *commander.Context) commander.Error!void {
    const worker_id = ctx.getPositional("worker_id").?; // validated by commander
    const task_types = ctx.getVariadicArgs("task_types") orelse &.{};
    if (task_types.len == 0) return outcome.usage(ctx, "give at least one task type", .{});

    const namespace = cli_config.getNamespace(ctx);
    const labels_str = ctx.getString("labels") orelse "";
    const labels: ?[]const u8 = if (labels_str.len > 0) labels_str else null;
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.action.workerRegister(&client, namespace, worker_id, task_types, labels) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("Registered worker: {s}\n", .{worker_id});
}

fn runWorkerAwait(ctx: *commander.Context) commander.Error!void {
    const task_types = ctx.getVariadicArgs("task_types") orelse &.{};
    if (task_types.len == 0) return outcome.usage(ctx, "give at least one task type", .{});

    const worker_id = ctx.getString("worker-id") orelse {
        return outcome.usage(ctx, "Missing --worker-id", .{});
    };

    const block = ctx.getUint("block") orelse 5000;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.action.workerAwait(&client, namespace, worker_id, task_types, @intCast(block)) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    const data = result.asRawData() orelse "";
    var diag: schema.Diagnostic = .{};
    const answer = action_ops.op("action.await").response.decodeAll(data, &diag) catch return outcome.malformed(ctx, "task assignment");
    const task = answer.task orelse {
        ctx.print("(no tasks)\n", .{});
        return;
    };
    var id_buf: [action_ops.Task.max_size]u8 = undefined;
    const id = action_ops.TaskId.of(task).format(&id_buf) catch return outcome.malformed(ctx, "task assignment");
    ctx.print("Task: {s}\n", .{id});
    ctx.print("Action: {s}\n", .{task.action});
    ctx.print("Attempt: {d}\n", .{task.attempt});
    if (task.caller_run) |caller| ctx.print("Caller: {s} ({s})\n", .{ caller, task.caller_workflow orelse "" });
    if (task.input.len > 0) ctx.print("Payload: {s}\n", .{task.input});
}

/// The task id a report names, or a usage error.
fn taskIdOf(ctx: *commander.Context) commander.Error!action_ops.TaskId {
    const text = ctx.getPositional("task_id").?; // validated by commander
    return action_ops.TaskId.parse(text) orelse outcome.usage(ctx, "task id {s} isn't one await printed: run:deliveries:term:index:action", .{text});
}

fn runWorkerComplete(ctx: *commander.Context) commander.Error!void {
    const id = try taskIdOf(ctx);
    const worker_id = ctx.getString("worker-id") orelse return outcome.usage(ctx, "Missing --worker-id", .{});
    const namespace = cli_config.getNamespace(ctx);
    var client = Client.init(ctx.allocator, cli_config.getEndpoint(ctx));
    defer client.deinit();
    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);
    const named = ctx.getString("outcome") orelse "";
    if (named.len > action_ops.MAX_OUTCOME) return outcome.usage(ctx, "--outcome is at most {d} bytes, not {d}", .{ action_ops.MAX_OUTCOME, named.len });
    var result = client_mod.action.workerComplete(&client, namespace, worker_id, id, named, ctx.getString("result") orelse "") catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();
    try outcome.check(ctx, result);
    ctx.print("OK\n", .{});
}

fn runWorkerFail(ctx: *commander.Context) commander.Error!void {
    const id = try taskIdOf(ctx);
    const worker_id = ctx.getString("worker-id") orelse return outcome.usage(ctx, "Missing --worker-id", .{});
    const namespace = cli_config.getNamespace(ctx);
    var client = Client.init(ctx.allocator, cli_config.getEndpoint(ctx));
    defer client.deinit();
    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);
    var result = client_mod.action.workerFail(&client, namespace, worker_id, id, ctx.getString("error") orelse "", ctx.getBool("retry")) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();
    try outcome.check(ctx, result);
    ctx.print("OK\n", .{});
}

fn runWorkerTouch(ctx: *commander.Context) commander.Error!void {
    const id = try taskIdOf(ctx);
    const worker_id = ctx.getString("worker-id") orelse return outcome.usage(ctx, "Missing --worker-id", .{});
    const namespace = cli_config.getNamespace(ctx);
    var client = Client.init(ctx.allocator, cli_config.getEndpoint(ctx));
    defer client.deinit();
    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);
    var result = client_mod.action.workerTouch(&client, namespace, worker_id, id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();
    try outcome.check(ctx, result);
    var diag: schema.Diagnostic = .{};
    const kept = action_ops.op("action.touch").response.decodeAll(result.asRawData() orelse "", &diag) catch return outcome.malformed(ctx, "touch answer");
    ctx.print("Expires at: {d}\n", .{kept.expires_at_ms});
}

fn runWorkerList(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    // 0 shows every worker.
    const most = ctx.getUint("limit") orelse 0;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    const format = cli_output.getFormat(ctx);
    var table = cli_output.Table.init(ctx.allocator);
    defer table.deinit();
    for ([_][]const u8{ "WORKER ID", "TYPE", "STATUS", "LOAD", "COMPLETED", "FAILED", "MACHINE" }) |h| {
        table.addColumn(h, .left) catch return outcome.malformed(ctx, "worker.list answer");
    }

    // JSON and raw rows are kept until the last page, so a page that fails
    // leaves no half-printed list behind.
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(ctx.allocator);

    var cursor_buf: [worker_ops.WorkerId.bound]u8 = undefined;
    var cursor: []const u8 = "";
    var shown: u64 = 0;
    while (true) {
        const page_limit: u32 = if (most == 0) worker_ops.MAX_LIST else @intCast(@min(most - shown, worker_ops.MAX_LIST));
        var result = client_mod.action.workerList(&client, namespace, page_limit, cursor) catch |err| return outcome.requestFailed(ctx, err);
        defer result.deinit();
        try outcome.check(ctx, result);

        var diag: schema.Diagnostic = .{};
        const page = worker_ops.op("worker.list").response.decodeAll(result.data, &diag) catch return outcome.malformed(ctx, "worker.list answer");
        var it = page.workers.iterator();
        while (it.next() catch return outcome.malformed(ctx, "worker.list answer")) |w| {
            switch (format) {
                .json => {
                    if (shown > 0) out.append(ctx.allocator, ',') catch return outcome.malformed(ctx, "worker.list answer");
                    out.print(ctx.allocator, "{{\"worker_id\":{f},\"type\":\"{s}\",\"status\":\"{s}\",\"current_load\":{d},\"max_concurrency\":{d},\"tasks_completed\":{d},\"tasks_failed\":{d},\"registered_at_ms\":{d},\"last_heartbeat_ms\":{d},\"machine_id\":", .{ cli_output.jsonString(w.id), @tagName(w.worker_type), @tagName(w.status), w.current_load, w.max_concurrency, w.tasks_completed, w.tasks_failed, w.registered_at_ms, w.last_heartbeat_ms }) catch return outcome.malformed(ctx, "worker.list answer");
                    (if (w.machine_id) |m| out.print(ctx.allocator, "{f}}}", .{cli_output.jsonString(m)}) else out.appendSlice(ctx.allocator, "null}")) catch return outcome.malformed(ctx, "worker.list answer");
                },
                .raw => out.print(ctx.allocator, "{s}\n", .{w.id}) catch return outcome.malformed(ctx, "worker.list answer"),
                .table => {
                    var load: [24]u8 = undefined;
                    var done: [24]u8 = undefined;
                    var failed: [24]u8 = undefined;
                    table.addRow(&.{
                        w.id,
                        @tagName(w.worker_type),
                        @tagName(w.status),
                        std.fmt.bufPrint(&load, "{d}/{d}", .{ w.current_load, w.max_concurrency }) catch unreachable,
                        std.fmt.bufPrint(&done, "{d}", .{w.tasks_completed}) catch unreachable,
                        std.fmt.bufPrint(&failed, "{d}", .{w.tasks_failed}) catch unreachable,
                        w.machine_id orelse "",
                    }) catch return outcome.malformed(ctx, "worker.list answer");
                },
            }
            shown += 1;
        }
        const next = page.cursor orelse break;
        if (most != 0 and shown >= most) break;
        // Pages run in id order: a cursor that doesn't move on would ask
        // for the same page forever.
        if (std.mem.order(u8, next, cursor) != .gt) return outcome.malformed(ctx, "worker.list answer (its cursor didn't move on)");
        @memcpy(cursor_buf[0..next.len], next);
        cursor = cursor_buf[0..next.len];
    }
    switch (format) {
        .json => ctx.print("[{s}]\n", .{out.items}),
        .raw => ctx.print("{s}", .{out.items}),
        .table => if (shown == 0) ctx.print("(no workers)\n", .{}) else table.print(ctx),
    }
}

fn runWorkerDrain(ctx: *commander.Context) commander.Error!void {
    const worker_id = ctx.getPositional("worker_id").?; // validated by commander
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.action.workerDrain(&client, namespace, worker_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("Draining worker: {s}\n", .{worker_id});
}

fn runWorkerInfo(ctx: *commander.Context) commander.Error!void {
    const worker_id = ctx.getPositional("worker_id").?;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.action.workerInfo(&client, namespace, worker_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    var diag: schema.Diagnostic = .{};
    const info = worker_ops.op("worker.info").response.decodeAll(result.data, &diag) catch return outcome.malformed(ctx, "worker.info answer");
    const w = info.worker;

    ctx.print("Worker ID:       {s}\n", .{w.id});
    ctx.print("Status:          {s}\n", .{@tagName(w.status)});
    ctx.print("Type:            {s}\n", .{@tagName(w.worker_type)});
    ctx.print("Current Load:    {d} / {d}\n", .{ w.current_load, w.max_concurrency });
    ctx.print("Tasks Completed: {d}\n", .{w.tasks_completed});
    ctx.print("Tasks Failed:    {d}\n", .{w.tasks_failed});
    ctx.print("Registered:      {d}\n", .{w.registered_at_ms});
    ctx.print("Last Heartbeat:  {d}\n", .{w.last_heartbeat_ms});

    if (info.processes.count > 0) {
        ctx.print("\nProcesses ({d}):\n", .{info.processes.count});
        var it = info.processes.iterator();
        while (it.next() catch return outcome.malformed(ctx, "worker.info answer")) |p| {
            ctx.print("  {s} ({s}) — runs: {d}, fails: {d}, last_run: {d}\n", .{ p.name, @tagName(p.kind), p.run_count, p.fail_count, p.last_run_at_ms });
        }
    }
    if (info.metadata) |meta| ctx.print("Metadata:        {s}\n", .{meta});
    if (w.machine_id) |mid| ctx.print("Machine ID:      {s}\n", .{mid});
}

// ==================== Testing ====================

test "create action command" {
    const allocator = std.testing.allocator;

    const cmd = try createActionCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("action", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 4);
}

test "create worker command" {
    const allocator = std.testing.allocator;

    const cmd = try createWorkerCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("worker", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 3);
}
