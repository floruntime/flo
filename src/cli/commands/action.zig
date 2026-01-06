//! Action & Worker commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo action register <name> [--type user|wasm] [--owner <owner>] [--timeout <ms>]
//!   flo action invoke <name> <input> [--priority <0-255>] [--idempotency-key <key>]
//!   flo action status <run_id>
//!   flo action list [--limit <n>]
//!   flo action delete <name>

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const wire = @import("../../util/wire.zig");
const WireReader = wire.WireReader;

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
                    "flo action register handler --type wasm --wasm-module rules.wasm",
                    "flo action register process --owner myteam --retries 3",
                })
                .arg("name", "Action name")
                .stringFlag("type", 't', "user", "Action type: user, wasm")
                .stringFlag("owner", 'o', "cli", "Owner/team name")
                .stringFlag("wasm-module", 'w', "", "Path to WASM module file (for --type wasm)")
                .uintFlag("timeout", 0, 30000, "Execution timeout (ms)")
                .uintFlag("retries", 'r', 0, "Max retry attempts")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runRegister)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("invoke")
                .about("Invoke an action")
                .aliases(&.{"call"})
                .examples(&.{
                    "flo action invoke myaction '{\"key\":\"value\"}'",
                    "flo action invoke process --priority 100 '{\"data\":1}'",
                    "flo action invoke handler --idempotency-key req-123 input",
                    "flo action invoke render '{\"frame\":1}' --labels '{\"gpu\":true}'",
                })
                .arg("name", "Action name")
                .arg("input", "Input payload (JSON)")
                .uintFlag("priority", 'p', 0, "Priority (0-255)")
                .stringFlag("idempotency-key", 'k', "", "Idempotency key for dedup")
                .stringFlag("labels", 'l', "", "Required worker labels (JSON, e.g. '{\"gpu\":true}')")
                .boolFlag("async", 'a', "Don't wait for result")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runInvoke)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("status")
                .about("Get execution status")
                .arg("run_id", "Run ID to check")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runStatus)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List registered actions")
                .aliases(&.{"ls"})
                .uintFlag("limit", 'l', 100, "Maximum actions to show")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runList)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("delete")
                .about("Delete an action")
                .aliases(&.{"rm"})
                .arg("name", "Action name to delete")
                .boolFlag("force", 'f', "Force delete even if running")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runDelete)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("logs")
                .about("View action execution logs")
                .arg("name", "Action name")
                .uintFlag("limit", 'l', 50, "Maximum log entries")
                .uintFlag("run", 'r', 0, "Specific run ID")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runLogs)),
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
                .arg("task_types", "Task types to handle")
                .stringFlag("labels", 'l', "", "Worker labels (JSON, e.g. '{\"gpu\":true}')")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runWorkerRegister)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("await")
                .about("Wait for tasks")
                .arg("task_types", "Task types to wait for")
                .stringFlag("worker-id", 'w', "", "Worker ID (required)")
                .uintFlag("block", 'b', 5000, "Block timeout (ms)")
                .uintFlag("timeout", 't', 30000, "Visibility timeout (ms)")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runWorkerAwait)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("complete")
                .about("Complete a task successfully")
                .arg("task_id", "Task ID")
                .stringFlag("worker-id", 'w', "", "Worker ID (required)")
                .stringFlag("action", 'a', "", "Action name (required for routing)")
                .stringFlag("result", 0, "", "Result payload (JSON)")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runWorkerComplete)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("fail")
                .about("Fail a task")
                .arg("task_id", "Task ID")
                .stringFlag("worker-id", 'w', "", "Worker ID (required)")
                .stringFlag("action", 'a', "", "Action name (required for routing)")
                .stringFlag("error", 0, "", "Error message")
                .boolFlag("retry", 'r', "Allow retry")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runWorkerFail)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("touch")
                .about("Extend task visibility timeout")
                .arg("task_id", "Task ID")
                .stringFlag("worker-id", 'w', "", "Worker ID (required)")
                .stringFlag("action", 'a', "", "Action name (required for routing)")
                .uintFlag("extend", 0, 30000, "Extend by (ms)")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runWorkerTouch)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List registered workers")
                .aliases(&.{"ls"})
                .uintFlag("limit", 'l', 100, "Maximum workers to show")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runWorkerList)),
        )
        .build();
}

/// Get endpoint from flags or config
fn getEndpoint(ctx: *commander.Context) []const u8 {
    if (ctx.getString("endpoint")) |ep| {
        if (ep.len > 0) return ep;
    }
    return "127.0.0.1:9000";
}

fn runRegister(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?; // validated by commander

    const action_type_str = ctx.getString("type") orelse "user";
    const owner = ctx.getString("owner") orelse "cli";
    const wasm_module_path = ctx.getString("wasm-module");
    const timeout = ctx.getUint("timeout") orelse 30000;
    const retries = ctx.getUint("retries") orelse 0;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

    // Convert type string to enum value
    var action_type: u8 = if (std.mem.eql(u8, action_type_str, "wasm")) 1 else 0;

    // Read WASM module file if provided
    var wasm_bytes: ?[]const u8 = null;
    defer if (wasm_bytes) |wb| ctx.allocator.free(wb);

    if (wasm_module_path) |path| {
        if (path.len > 0) {
            // Auto-set type to wasm when --wasm-module is provided
            action_type = 1;

            const file = std.fs.cwd().openFile(path, .{}) catch {
                ctx.printErr("Cannot open WASM module: {s}\n", .{path});
                return;
            };
            defer file.close();

            const stat = file.stat() catch {
                ctx.printErr("Cannot stat WASM module: {s}\n", .{path});
                return;
            };

            if (stat.size > 10 * 1024 * 1024) {
                ctx.printErr("WASM module too large ({d} bytes, max 10MB)\n", .{stat.size});
                return;
            }

            wasm_bytes = file.readToEndAlloc(ctx.allocator, 10 * 1024 * 1024) catch {
                ctx.printErr("Failed to read WASM module: {s}\n", .{path});
                return;
            };
        }
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    // register(client, namespace, action_name, action_type, owner, timeout_ms, max_retries, retry_delay_ms, wasm_bytes)
    var result = client_mod.action.register(&client, namespace, name, action_type, owner, @intCast(timeout), @intCast(retries), null, wasm_bytes) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    if (wasm_bytes) |wb| {
        ctx.print("Registered WASM action: {s} ({d} bytes)\n", .{ name, wb.len });
    } else {
        ctx.print("Registered action: {s}\n", .{name});
    }
}

fn runInvoke(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?; // validated by commander
    const input = ctx.getPositional("input").?; // validated by commander

    const priority_val = ctx.getUint("priority") orelse 0;
    const priority: u8 = if (priority_val > 255) 255 else @intCast(priority_val);
    const idempotency_key = ctx.getString("idempotency-key");
    const labels_str = ctx.getString("labels") orelse "";
    const required_labels: ?[]const u8 = if (labels_str.len > 0) labels_str else null;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.action.invoke(&client, namespace, name, input, priority, idempotency_key, required_labels) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    // Response data is the raw run_id string (no wire encoding for this response)
    if (result.asRawData()) |run_id| {
        if (run_id.len > 0) {
            ctx.print("Result: {s}\n", .{run_id});
            return;
        }
    }
    ctx.print("OK\n", .{});
}

fn runStatus(ctx: *commander.Context) commander.Error!void {
    const run_id = ctx.getPositional("run_id").?; // validated by commander

    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // status(client, namespace, run_id)
    var result = client_mod.action.status(&client, namespace, run_id) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.printErr("Run not found: {s}\n", .{run_id});
        return error.CommandFailed;
    }

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    ctx.print("Run: {s}\n", .{run_id});
    if (result.asString()) |data| {
        ctx.print("{s}\n", .{data});
    }
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const limit = ctx.getUint("limit") orelse 100;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

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
        var result = client_mod.action.list(&client, namespace, @intCast(limit), null, cursor) catch |err| {
            ctx.printErr("Request failed: {}\n", .{err});
            return;
        };
        defer result.deinit();

        if (result.isError()) {
            ctx.printErr("Error: {s}\n", .{result.errorMessage()});
            return;
        }

        // action_list returns scan format:
        // [count:u32] ([key_len:u16][key][value_len:u32][value])* [has_more:u8] [cursor_len:u16][cursor]?
        const data = result.asRawData() orelse break;
        if (data.len < 4) break;

        var reader = WireReader.init(data);
        const count = reader.readU32() orelse break;

        // Parse entries
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            const key_len = reader.readU16() orelse break;
            const key = reader.readSlice(key_len) orelse break;
            // Skip value (keys_only but value field still present)
            const val_len = reader.readU32() orelse break;
            _ = reader.readSlice(val_len);

            // Extract action name from key (format: _action:{name})
            const name = if (std.mem.startsWith(u8, key, "_action:"))
                key["_action:".len..]
            else
                key;

            const name_copy = ctx.allocator.dupe(u8, name) catch break;
            all_names.append(ctx.allocator, name_copy) catch {
                ctx.allocator.free(name_copy);
                break;
            };

            if (all_names.items.len >= limit) break;
        }

        // Read has_more flag
        const has_more = (reader.readU8() orelse 0) != 0;

        // Read next cursor
        const cursor_len = reader.readU16() orelse 0;
        const next_cursor = if (cursor_len > 0) reader.readSlice(cursor_len) else null;

        // Free previous cursor and copy new one
        if (cursor_owned) |c| ctx.allocator.free(c);
        cursor_owned = null;

        if (!has_more or next_cursor == null) break;

        // Copy cursor for next iteration
        cursor_owned = ctx.allocator.dupe(u8, next_cursor.?) catch break;
        cursor = cursor_owned;
    }

    // Output results
    if (all_names.items.len == 0) {
        ctx.print("(no actions)\n", .{});
    } else {
        for (all_names.items) |name| {
            ctx.print("{s}\n", .{name});
        }
    }
}

fn runDelete(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?; // validated by commander

    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);
    _ = ctx.getBool("force"); // Not yet used

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    // delete(client, namespace, action_name)
    var result = client_mod.action.delete(&client, namespace, name) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    ctx.print("Deleted action: {s}\n", .{name});
}

fn runLogs(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?; // validated by commander

    const limit = ctx.getUint("limit") orelse 50;
    const run_id = ctx.getUint("run");
    const namespace = ctx.getString("namespace") orelse "default";
    _ = namespace;
    _ = limit;
    _ = run_id;

    // Logs endpoint not yet in client API
    ctx.print("Logs for action: {s}\n", .{name});
    ctx.print("Note: Action logs not yet implemented in client API.\n", .{});
}

// Worker handlers

fn runWorkerRegister(ctx: *commander.Context) commander.Error!void {
    const worker_id = ctx.getPositional("worker_id").?; // validated by commander
    const task_types_str = ctx.getPositional("task_types").?; // validated by commander

    const namespace = ctx.getString("namespace") orelse "default";
    const labels_str = ctx.getString("labels") orelse "";
    const labels: ?[]const u8 = if (labels_str.len > 0) labels_str else null;
    const endpoint = getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    const task_types = &[_][]const u8{task_types_str};
    var result = client_mod.action.workerRegister(&client, namespace, worker_id, task_types, labels) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    ctx.print("Registered worker: {s}\n", .{worker_id});
}

fn runWorkerAwait(ctx: *commander.Context) commander.Error!void {
    const task_types_str = ctx.getPositional("task_types").?; // validated by commander

    const worker_id = ctx.getString("worker-id") orelse {
        ctx.printErr("Error: Missing --worker-id\n", .{});
        return;
    };

    const block = ctx.getUint("block") orelse 5000;
    const timeout = ctx.getUint("timeout") orelse 30000;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // workerAwait(client, namespace, worker_id, task_types, block_ms, timeout_ms, max_tasks)
    const task_types = &[_][]const u8{task_types_str};
    var result = client_mod.action.workerAwait(&client, namespace, worker_id, task_types, @intCast(block), @intCast(timeout), null) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    // task_assignment response format: [task_id_len:u16][task_id][payload]
    // Empty data means no tasks
    if (result.asRawData()) |data| {
        if (data.len == 0) {
            ctx.print("(no tasks)\n", .{});
            return;
        }

        // Parse task_id_len
        if (data.len < 2) {
            ctx.print("(no tasks)\n", .{});
            return;
        }

        const task_id_len = std.mem.readInt(u16, data[0..2], .little);
        if (data.len < 2 + task_id_len) {
            ctx.printErr("Error: Invalid task assignment format\n", .{});
            return error.CommandFailed;
        }

        const task_id = data[2..][0..task_id_len];
        const payload = data[2 + task_id_len ..];

        ctx.print("Task: {s}\n", .{task_id});
        if (payload.len > 0) {
            ctx.print("Payload: {s}\n", .{payload});
        }
    } else {
        ctx.print("(no tasks)\n", .{});
    }
}

fn runWorkerComplete(ctx: *commander.Context) commander.Error!void {
    const task_id = ctx.getPositional("task_id").?; // validated by commander

    const worker_id = ctx.getString("worker-id") orelse {
        ctx.printErr("Error: Missing --worker-id\n", .{});
        return error.CommandFailed;
    };

    const action_name = ctx.getString("action") orelse "";
    const result_payload = ctx.getString("result") orelse "";
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // workerComplete(client, namespace, worker_id, action_name, task_id, result)
    var result = client_mod.action.workerComplete(&client, namespace, worker_id, action_name, task_id, result_payload) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("OK\n", .{});
}

fn runWorkerFail(ctx: *commander.Context) commander.Error!void {
    const task_id = ctx.getPositional("task_id").?; // validated by commander

    const worker_id = ctx.getString("worker-id") orelse {
        ctx.printErr("Error: Missing --worker-id\n", .{});
        return error.CommandFailed;
    };

    const action_name = ctx.getString("action") orelse "";
    const error_msg = ctx.getString("error") orelse "";
    const allow_retry = ctx.getBool("retry");
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };
    // workerFail(client, namespace, worker_id, action_name, task_id, error_message, retry)
    var result = client_mod.action.workerFail(&client, namespace, worker_id, action_name, task_id, error_msg, allow_retry) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("OK\n", .{});
}

fn runWorkerTouch(ctx: *commander.Context) commander.Error!void {
    const task_id = ctx.getPositional("task_id").?; // validated by commander

    const worker_id = ctx.getString("worker-id") orelse {
        ctx.printErr("Error: Missing --worker-id\n", .{});
        return error.CommandFailed;
    };

    const action_name = ctx.getString("action") orelse "";
    const extend = ctx.getUint("extend") orelse 30000;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // workerTouch(client, namespace, worker_id, action_name, task_id, extend_ms)
    var result = client_mod.action.workerTouch(&client, namespace, worker_id, action_name, task_id, @intCast(extend)) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("OK\n", .{});
}

fn runWorkerList(ctx: *commander.Context) commander.Error!void {
    const limit = ctx.getUint("limit") orelse 100;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    // Collect all worker IDs across shards using cursor-based shard walking
    var all_workers: std.ArrayList([]const u8) = .empty;
    defer {
        for (all_workers.items) |w| {
            ctx.allocator.free(w);
        }
        all_workers.deinit(ctx.allocator);
    }

    var cursor: ?[]const u8 = null;
    var cursor_owned: ?[]u8 = null;
    defer if (cursor_owned) |c| ctx.allocator.free(c);

    // Walk all shards until no more data
    while (all_workers.items.len < limit) {
        var result = client_mod.action.workerList(&client, namespace, @intCast(limit), cursor) catch |err| {
            ctx.printErr("Request failed: {}\n", .{err});
            return;
        };
        defer result.deinit();

        if (result.isError()) {
            ctx.printErr("Error: {s}\n", .{result.errorMessage()});
            return;
        }

        // worker_list returns scan format:
        // [count:u32] ([key_len:u16][key][value_len:u32][value])* [has_more:u8] [cursor_len:u16][cursor]?
        const data = result.asRawData() orelse break;
        if (data.len < 4) break;

        var reader = WireReader.init(data);
        const count = reader.readU32() orelse break;

        // Parse entries
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            const key_len = reader.readU16() orelse break;
            const key = reader.readSlice(key_len) orelse break;
            // Skip value (keys_only but value field still present)
            const val_len = reader.readU32() orelse break;
            _ = reader.readSlice(val_len);

            // Extract worker ID from key (format: _worker:{worker_id})
            const worker_id = if (std.mem.startsWith(u8, key, "_worker:"))
                key["_worker:".len..]
            else
                key;

            const id_copy = ctx.allocator.dupe(u8, worker_id) catch break;
            all_workers.append(ctx.allocator, id_copy) catch {
                ctx.allocator.free(id_copy);
                break;
            };

            if (all_workers.items.len >= limit) break;
        }

        // Read has_more flag
        const has_more = (reader.readU8() orelse 0) != 0;

        // Read next cursor
        const cursor_len = reader.readU16() orelse 0;
        const next_cursor = if (cursor_len > 0) reader.readSlice(cursor_len) else null;

        // Free previous cursor and copy new one
        if (cursor_owned) |c| ctx.allocator.free(c);
        cursor_owned = null;

        if (!has_more or next_cursor == null) break;

        // Copy cursor for next iteration
        cursor_owned = ctx.allocator.dupe(u8, next_cursor.?) catch break;
        cursor = cursor_owned;
    }

    // Output results
    if (all_workers.items.len == 0) {
        ctx.print("(no workers)\n", .{});
    } else {
        for (all_workers.items) |w| {
            ctx.print("{s}\n", .{w});
        }
    }
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
