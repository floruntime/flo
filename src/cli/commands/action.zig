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
const cli_config = @import("../config.zig");

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
                .name("runs")
                .about("List action runs/tasks")
                .aliases(&.{"tasks"})
                .arg("name", "Action name")
                .uintFlag("limit", 'l', 50, "Maximum runs to show")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
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
        .subcommand(
            commander.newBuilder(allocator)
                .name("drain")
                .about("Drain a worker (stop new task assignments)")
                .examples(&.{
                    "flo worker drain worker-1",
                    "flo worker drain worker-1 -n my-namespace",
                })
                .arg("worker_id", "Worker identifier to drain")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
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
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runWorkerInfo)),
        )
        .build();
}

fn runRegister(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?; // validated by commander

    const action_type_str = ctx.getString("type") orelse "user";
    const owner = ctx.getString("owner") orelse "cli";
    const wasm_module_path = ctx.getString("wasm-module");
    const timeout = ctx.getUint("timeout") orelse 30000;
    const retries = ctx.getUint("retries") orelse 0;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

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
    const endpoint = cli_config.getEndpoint(ctx);

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
    const endpoint = cli_config.getEndpoint(ctx);

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
    if (result.asRawData()) |data| {
        printActionRunStatus(ctx, data);
    }
}

/// Parse and print the binary action_run_status wire format.
/// Format: [run_id_len:u32][run_id][status:u8][created_at:i64]
///         [has_started:u8][started_at?:i64][has_completed:u8][completed_at?:i64]
///         [has_output:u8][output_len?:u32][output?][has_error:u8][error_len?:u32][error?]
///         [retry_count:u32]
fn printActionRunStatus(ctx: *commander.Context, data: []const u8) void {
    var off: usize = 0;

    // run_id
    const rid = readSlice(data, &off) orelse {
        ctx.print("{s}\n", .{data});
        return;
    };
    _ = rid; // already printed by caller as "Run: <run_id>"

    // status
    if (off >= data.len) return;
    const status_byte = data[off];
    off += 1;
    const status_str: []const u8 = switch (status_byte) {
        0 => "pending",
        1 => "running",
        2 => "completed",
        3 => "failed",
        4 => "cancelled",
        5 => "timed_out",
        else => "unknown",
    };
    ctx.print("status: {s}\n", .{status_str});

    // created_at
    if (off + 8 > data.len) return;
    const created_at = std.mem.readInt(i64, data[off..][0..8], .little);
    off += 8;
    ctx.print("created_at: {d}\n", .{created_at});

    // started_at (optional)
    if (readOptionalI64(data, &off)) |started| {
        ctx.print("started_at: {d}\n", .{started});
    }

    // completed_at (optional)
    if (readOptionalI64(data, &off)) |completed| {
        ctx.print("completed_at: {d}\n", .{completed});
    }

    // output (optional slice)
    if (readOptionalSlice(data, &off)) |output| {
        ctx.print("output: {s}\n", .{output});
    }

    // error_message (optional slice)
    if (readOptionalSlice(data, &off)) |err_msg| {
        ctx.print("error: {s}\n", .{err_msg});
    }

    // retry_count
    if (off + 4 <= data.len) {
        const retries = std.mem.readInt(u32, data[off..][0..4], .little);
        if (retries > 0) {
            ctx.print("retry_count: {d}\n", .{retries});
        }
    }
}

fn readSlice(data: []const u8, off: *usize) ?[]const u8 {
    if (off.* + 4 > data.len) return null;
    const len = std.mem.readInt(u32, data[off.*..][0..4], .little);
    off.* += 4;
    if (off.* + len > data.len) return null;
    const slice = data[off.* .. off.* + len];
    off.* += len;
    return slice;
}

fn readOptionalI64(data: []const u8, off: *usize) ?i64 {
    if (off.* >= data.len) return null;
    const has = data[off.*];
    off.* += 1;
    if (has == 0) return null;
    if (off.* + 8 > data.len) return null;
    const val = std.mem.readInt(i64, data[off.*..][0..8], .little);
    off.* += 8;
    return val;
}

fn readOptionalSlice(data: []const u8, off: *usize) ?[]const u8 {
    if (off.* >= data.len) return null;
    const has = data[off.*];
    off.* += 1;
    if (has == 0) return null;
    return readSlice(data, off);
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const limit = ctx.getUint("limit") orelse 100;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

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
        var result = client_mod.action.list(&client, namespace, null, cursor) catch |err| {
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
    const endpoint = cli_config.getEndpoint(ctx);
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

fn runRuns(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?; // validated by commander
    const namespace = ctx.getString("namespace") orelse "default";
    const limit: u32 = @intCast(ctx.getUint("limit") orelse 50);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.action.listRuns(&client, namespace, name, limit) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    const data = result.asRawData() orelse {
        ctx.print("(no runs)\n", .{});
        return;
    };
    if (data.len < 4) {
        ctx.print("(no runs)\n", .{});
        return;
    }

    var reader = WireReader.init(data);
    const count = reader.readU32() orelse {
        ctx.print("(no runs)\n", .{});
        return;
    };

    if (count == 0) {
        ctx.print("(no runs)\n", .{});
        return;
    }

    // Table header
    ctx.print("{s:<12} {s:<20} {s:<12} {s:<24} {s:<24} {s:<24}\n", .{
        "RUN ID", "ACTION", "STATUS", "CREATED", "STARTED", "COMPLETED",
    });
    ctx.print("{s}\n", .{"-" ** 116});

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        // run_id
        const rid_len = reader.readU16() orelse break;
        const run_id = reader.readSlice(rid_len) orelse break;

        // action_name
        const aname_len = reader.readU16() orelse break;
        const action_name = reader.readSlice(aname_len) orelse break;

        // status
        const status_byte = reader.readU8() orelse break;
        const status_str: []const u8 = switch (status_byte) {
            0 => "pending",
            1 => "running",
            2 => "completed",
            3 => "failed",
            4 => "cancelled",
            5 => "timed_out",
            else => "unknown",
        };

        // created_at
        const created_at = reader.readI64() orelse break;

        // started_at (optional)
        const has_started = reader.readU8() orelse break;
        var started_at: ?i64 = null;
        if (has_started == 1) {
            started_at = reader.readI64();
        }

        // completed_at (optional)
        const has_completed = reader.readU8() orelse break;
        var completed_at: ?i64 = null;
        if (has_completed == 1) {
            completed_at = reader.readI64();
        }

        // Format timestamps as relative durations or raw
        var created_buf: [24]u8 = undefined;
        const created_str = formatTimestamp(created_at, &created_buf);
        var started_buf: [24]u8 = undefined;
        const started_str = if (started_at) |s| formatTimestamp(s, &started_buf) else "—";
        var completed_buf: [24]u8 = undefined;
        const completed_str = if (completed_at) |c| formatTimestamp(c, &completed_buf) else "—";

        ctx.print("{s:<12} {s:<20} {s:<12} {s:<24} {s:<24} {s:<24}\n", .{
            run_id, action_name, status_str, created_str, started_str, completed_str,
        });
    }
}

fn formatTimestamp(ms: i64, buf: *[24]u8) []const u8 {
    const now = std.time.milliTimestamp();
    const diff = now - ms;

    if (diff < 0) {
        return std.fmt.bufPrint(buf, "{d}ms", .{ms}) catch "?";
    }
    if (diff < 1000) {
        return std.fmt.bufPrint(buf, "{d}ms ago", .{diff}) catch "?";
    }
    if (diff < 60_000) {
        return std.fmt.bufPrint(buf, "{d}s ago", .{@divTrunc(diff, 1000)}) catch "?";
    }
    if (diff < 3_600_000) {
        return std.fmt.bufPrint(buf, "{d}m ago", .{@divTrunc(diff, 60_000)}) catch "?";
    }
    return std.fmt.bufPrint(buf, "{d}h ago", .{@divTrunc(diff, 3_600_000)}) catch "?";
}

// Worker handlers

fn runWorkerRegister(ctx: *commander.Context) commander.Error!void {
    const worker_id = ctx.getPositional("worker_id").?; // validated by commander
    const task_types_str = ctx.getPositional("task_types").?; // validated by commander

    const namespace = ctx.getString("namespace") orelse "default";
    const labels_str = ctx.getString("labels") orelse "";
    const labels: ?[]const u8 = if (labels_str.len > 0) labels_str else null;
    const endpoint = cli_config.getEndpoint(ctx);

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
    const endpoint = cli_config.getEndpoint(ctx);

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

    // task_assignment response format:
    //   [task_id_len:u16][task_id][task_type_len:u16][task_type][created_at:i64][attempt:u32][payload]
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

        var pos: usize = 0;

        const task_id_len = std.mem.readInt(u16, data[0..2], .little);
        pos += 2;
        if (data.len < pos + task_id_len) {
            ctx.printErr("Error: Invalid task assignment format\n", .{});
            return error.CommandFailed;
        }

        const task_id = data[pos..][0..task_id_len];
        pos += task_id_len;

        // task_type
        var task_type: []const u8 = "";
        if (pos + 2 <= data.len) {
            const task_type_len = std.mem.readInt(u16, data[pos..][0..2], .little);
            pos += 2;
            if (pos + task_type_len <= data.len) {
                task_type = data[pos .. pos + task_type_len];
                pos += task_type_len;
            }
        }

        // created_at (i64) — skip for display
        if (pos + 8 <= data.len) pos += 8;

        // attempt (u32) — skip for display
        if (pos + 4 <= data.len) pos += 4;

        const payload = data[pos..];

        ctx.print("Task: {s}\n", .{task_id});
        if (task_type.len > 0) {
            ctx.print("Action: {s}\n", .{task_type});
        }
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
    const endpoint = cli_config.getEndpoint(ctx);

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
    const endpoint = cli_config.getEndpoint(ctx);

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
    const endpoint = cli_config.getEndpoint(ctx);

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
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    var result = client_mod.action.workerList(&client, namespace, 100, null) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    // Wire format: [count:u32](worker_record)*[has_more:u8][cursor_len:u16]
    const data = result.asRawData() orelse {
        ctx.print("(no workers)\n", .{});
        return;
    };
    if (data.len < 4) {
        ctx.print("(no workers)\n", .{});
        return;
    }

    var reader = WireReader.init(data);
    const count = reader.readU32() orelse {
        ctx.print("(no workers)\n", .{});
        return;
    };

    if (count == 0) {
        ctx.print("(no workers)\n", .{});
        return;
    }

    // Table header
    ctx.print("{s:<24} {s:<10} {s:<8} {s:<20} {s:<6} {s:<10} {s:<10}\n", .{
        "WORKER ID", "STATUS", "TYPE", "MACHINE", "LOAD", "COMPLETED", "FAILED",
    });
    ctx.print("{s}\n", .{"-" ** 90});

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        // Parse worker_record: [id_len:u16][id][type:u8][status:u8]...
        const id_len = reader.readU16() orelse break;
        const id = reader.readSlice(id_len) orelse break;
        const wtype = reader.readU8() orelse break;
        const wstatus = reader.readU8() orelse break;
        const tasks_completed = reader.readU64() orelse break;
        const tasks_failed = reader.readU64() orelse break;
        const current_load = reader.readU32() orelse break;
        _ = reader.readU32(); // max_concurrency
        _ = reader.readI64(); // registered_at
        _ = reader.readI64(); // last_heartbeat

        // Skip processes
        const proc_count = reader.readU16() orelse break;
        var pi: u16 = 0;
        while (pi < proc_count) : (pi += 1) {
            const nlen = reader.readU16() orelse break;
            _ = reader.readSlice(nlen); // name
            _ = reader.readU8(); // kind
            _ = reader.readU64(); // run_count
            _ = reader.readU64(); // fail_count
            _ = reader.readI64(); // last_run_at
        }

        // Skip metadata
        const has_meta = reader.readU8() orelse break;
        if (has_meta == 1) {
            const mlen = reader.readU16() orelse break;
            _ = reader.readSlice(mlen);
        }

        // Skip machine_id but capture it
        var machine: []const u8 = "—";
        const has_mid = reader.readU8() orelse break;
        if (has_mid == 1) {
            const midlen = reader.readU16() orelse break;
            machine = reader.readSlice(midlen) orelse "—";
        }

        const type_str: []const u8 = if (wtype == 0) "action" else "stream";
        const status_str: []const u8 = switch (wstatus) {
            0 => "active",
            1 => "idle",
            2 => "draining",
            3 => "unhealthy",
            else => "unknown",
        };

        ctx.print("{s:<24} {s:<10} {s:<8} {s:<20} {d:<6} {d:<10} {d:<10}\n", .{
            id, status_str, type_str, machine, current_load, tasks_completed, tasks_failed,
        });
    }
}

fn runWorkerDrain(ctx: *commander.Context) commander.Error!void {
    const worker_id = ctx.getPositional("worker_id").?; // validated by commander
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    var result = client_mod.action.workerDrain(&client, namespace, worker_id) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    ctx.print("Draining worker: {s}\n", .{worker_id});
}

fn runWorkerInfo(ctx: *commander.Context) commander.Error!void {
    const worker_id = ctx.getPositional("worker_id").?;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    var result = client_mod.action.workerInfo(&client, namespace, worker_id) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    const data = result.asRawData() orelse {
        ctx.printErr("No data returned\n", .{});
        return;
    };

    // Parse worker_record wire format
    var reader = WireReader.init(data);
    const id_len = reader.readU16() orelse return;
    const id = reader.readSlice(id_len) orelse return;
    const wtype = reader.readU8() orelse return;
    const wstatus = reader.readU8() orelse return;
    const tasks_completed = reader.readU64() orelse return;
    const tasks_failed = reader.readU64() orelse return;
    const current_load = reader.readU32() orelse return;
    const max_concurrency = reader.readU32() orelse return;
    const registered_at = reader.readI64() orelse return;
    const last_heartbeat = reader.readI64() orelse return;

    const type_str: []const u8 = if (wtype == 0) "action" else "stream";
    const status_str: []const u8 = switch (wstatus) {
        0 => "active",
        1 => "idle",
        2 => "draining",
        3 => "unhealthy",
        else => "unknown",
    };

    ctx.print("Worker ID:       {s}\n", .{id});
    ctx.print("Status:          {s}\n", .{status_str});
    ctx.print("Type:            {s}\n", .{type_str});
    ctx.print("Current Load:    {d} / {d}\n", .{ current_load, max_concurrency });
    ctx.print("Tasks Completed: {d}\n", .{tasks_completed});
    ctx.print("Tasks Failed:    {d}\n", .{tasks_failed});
    ctx.print("Registered:      {d}\n", .{registered_at});
    ctx.print("Last Heartbeat:  {d}\n", .{last_heartbeat});

    // Processes
    const proc_count = reader.readU16() orelse return;
    if (proc_count > 0) {
        ctx.print("\nProcesses ({d}):\n", .{proc_count});
        var pi: u16 = 0;
        while (pi < proc_count) : (pi += 1) {
            const nlen = reader.readU16() orelse break;
            const name = reader.readSlice(nlen) orelse break;
            const kind = reader.readU8() orelse break;
            const run_count = reader.readU64() orelse break;
            const fail_count = reader.readU64() orelse break;
            const last_run = reader.readI64() orelse break;
            const kind_str: []const u8 = if (kind == 0) "action" else "stream_consumer";
            ctx.print("  {s} ({s}) — runs: {d}, fails: {d}, last_run: {d}\n", .{
                name, kind_str, run_count, fail_count, last_run,
            });
        }
    }

    // Metadata
    const has_meta = reader.readU8() orelse return;
    if (has_meta == 1) {
        const mlen = reader.readU16() orelse return;
        const meta = reader.readSlice(mlen) orelse return;
        ctx.print("Metadata:        {s}\n", .{meta});
    }

    // Machine ID
    const has_mid = reader.readU8() orelse return;
    if (has_mid == 1) {
        const midlen = reader.readU16() orelse return;
        const mid = reader.readSlice(midlen) orelse return;
        ctx.print("Machine ID:      {s}\n", .{mid});
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
