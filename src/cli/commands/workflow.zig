//! Workflow commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo workflow create -f <definition.yaml>
//!   flo workflow start <name> [--version <ver>] [--input <json>] [--idempotency-key <key>]
//!   flo workflow signal <run_id> --type <signal_type> [--payload <json>]
//!   flo workflow status <run_id>
//!   flo workflow history <run_id> [--limit <n>]
//!   flo workflow list-runs [--workflow <name>] [--status <status>] [--search <query>] [--limit <n>]
//!   flo workflow cancel <run_id> [--reason <reason>]
//!   flo workflow definition <name> [--version <ver>]
//!   flo workflow disable <name> [--version <ver>]
//!   flo workflow enable <name> [--version <ver>]
//!   flo workflow list

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const cli_config = @import("../config.zig");
const output = @import("../output.zig");
const wf_parser = @import("../../workflow/parser.zig");
const wf_validator = @import("../../workflow/validator.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the workflow command tree
pub fn createWorkflowCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("workflow")
        .about("Workflow orchestration commands")
        .group("Data Commands")
        .longAbout(
            \\Manage durable workflows with state machine semantics.
            \\
            \\Workflows orchestrate actions and plans with automatic state persistence,
            \\signals, timers, and fault tolerance via circuit breakers.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("create")
                .about("Create a workflow from YAML definition")
                .examples(&.{
                    "flo workflow create -f order-processing.yaml",
                    "flo workflow create --file ./workflows/etl-pipeline.yaml",
                    "cat workflow.yaml | flo workflow create -f -",
                })
                .stringFlag("file", 'f', "", "YAML definition file (or - for stdin)")
                .action(wrapHandler(runCreate)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("start")
                .about("Start a new workflow run")
                .aliases(&.{"run"})
                .examples(&.{
                    "flo workflow start order-processing '{\"order_id\":123}'",
                    "flo workflow start etl-pipeline --version v2 '{\"source\":\"s3\"}'",
                    "flo workflow start payment --idempotency-key pay-123 '{}'",
                })
                .arg("name", "Workflow name")
                .arg("input", "Input payload (JSON)")
                .stringFlag("version", 'v', "latest", "Workflow version")
                .stringFlag("idempotency-key", 'k', "", "Idempotency key for dedup")
                .stringFlag("run-id", 'r', "", "Custom run ID (optional)")
                .action(wrapHandler(runStart)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("signal")
                .about("Send a signal to a running workflow")
                .examples(&.{
                    "flo workflow signal run-123 --type approve",
                    "flo workflow signal run-456 --type payment_received --payload '{\"amount\":100}'",
                })
                .arg("run_id", "Workflow run ID")
                .stringFlag("type", 't', "", "Signal type (required)")
                .stringFlag("payload", 'p', "", "Signal payload (JSON)")
                .action(wrapHandler(runSignal)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("status")
                .about("Get workflow run status")
                .aliases(&.{"get"})
                .arg("run_id", "Workflow run ID")
                .action(wrapHandler(runStatus)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("history")
                .about("Get workflow run history (events)")
                .arg("run_id", "Workflow run ID")
                .uintFlag("limit", 'l', 100, "Maximum events to show")
                .action(wrapHandler(runHistory)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list-runs")
                .about("List workflow runs")
                .aliases(&.{"runs"})
                .optionalArg("workflow_name", "Workflow name (shorthand for --workflow)")
                .stringFlag("workflow", 'w', "", "Filter by workflow name")
                .stringFlag("status", 's', "", "Filter by status (running, completed, failed, cancelled)")
                .stringFlag("search", 'q', "", "Search runs by keyword")
                .uintFlag("limit", 'l', 100, "Maximum runs to show")
                .action(wrapHandler(runListRuns)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("cancel")
                .about("Cancel a running workflow")
                .arg("run_id", "Workflow run ID")
                .stringFlag("reason", 'r', "", "Cancellation reason")
                .action(wrapHandler(runCancel)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("definition")
                .about("Get workflow definition")
                .aliases(&.{ "def", "show" })
                .arg("name", "Workflow name")
                .stringFlag("version", 'v', "", "Specific version (default: latest)")
                .action(wrapHandler(runGetDefinition)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("disable")
                .about("Disable a workflow (blocks new runs, pauses schedules)")
                .examples(&.{
                    "flo workflow disable order-processing",
                    "flo workflow disable order-processing --version 1.0.0",
                })
                .arg("name", "Workflow name")
                .stringFlag("version", 'v', "", "Specific version (default: all versions)")
                .action(wrapHandler(runDisable)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("enable")
                .about("Enable a workflow (allows runs, resumes schedules)")
                .examples(&.{
                    "flo workflow enable order-processing",
                    "flo workflow enable order-processing --version 1.0.0",
                })
                .arg("name", "Workflow name")
                .stringFlag("version", 'v', "", "Specific version (default: all versions)")
                .action(wrapHandler(runEnable)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List workflow definitions")
                .aliases(&.{"ls"})
                .examples(&.{
                    "flo workflow list",
                    "flo workflow list --namespace production",
                })
                .action(wrapHandler(runListDefinitions)),
        )
        .build();
}

// =============================================================================
// Command Handlers
// =============================================================================



fn runCreate(ctx: *commander.Context) commander.Error!void {
    const file_path = ctx.getString("file") orelse "";
    if (file_path.len == 0) {
        ctx.printErr("Error: --file is required\n", .{});
        return error.CommandFailed;
    }

    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    // Read definition from file or stdin
    var definition: []const u8 = undefined;
    var owned = false;

    if (std.mem.eql(u8, file_path, "-")) {
        // Read from stdin using posix read
        var stdin_buf: [1024 * 1024]u8 = undefined;
        var total_read: usize = 0;

        while (total_read < stdin_buf.len) {
            const bytes_read = std.posix.read(std.posix.STDIN_FILENO, stdin_buf[total_read..]) catch |err| {
                ctx.printErr("Failed to read stdin: {}\n", .{err});
                return error.CommandFailed;
            };
            if (bytes_read == 0) break;
            total_read += bytes_read;
        }

        if (total_read == 0) {
            ctx.printErr("Error: No input provided on stdin\n", .{});
            return error.CommandFailed;
        }

        // Copy to allocated memory
        const buf = ctx.allocator.alloc(u8, total_read) catch |err| {
            ctx.printErr("Failed to allocate memory: {}\n", .{err});
            return error.CommandFailed;
        };
        @memcpy(buf, stdin_buf[0..total_read]);
        definition = buf;
        owned = true;
    } else {
        // Read from file
        const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
            ctx.printErr("Failed to open file '{s}': {}\n", .{ file_path, err });
            return error.CommandFailed;
        };
        defer file.close();

        definition = file.readToEndAlloc(ctx.allocator, 1024 * 1024) catch |err| {
            ctx.printErr("Failed to read file: {}\n", .{err});
            return error.CommandFailed;
        };
        owned = true;
    }
    defer if (owned) ctx.allocator.free(definition);

    // Client-side pre-validation — catch errors before the server round-trip
    prevalidate: {
        var def = wf_parser.parseWorkflow(ctx.allocator, definition) catch {
            ctx.printErr("Error: invalid workflow definition (failed to parse)\n", .{});
            ctx.printErr("Hint: run 'flo validate workflow -f {s}' for detailed diagnostics\n", .{file_path});
            return error.CommandFailed;
        };
        defer def.deinit(ctx.allocator);

        var validation = wf_validator.validateWorkflow(ctx.allocator, &def) catch {
            // Non-fatal: fall through to server-side validation
            break :prevalidate;
        };
        defer validation.deinit();

        if (validation.hasErrors()) {
            ctx.printErr("Error: workflow definition has validation errors:\n", .{});
            for (validation.items()) |item| {
                if (item.severity == .@"error") {
                    ctx.printErr("  [{s}] {s}\n", .{ item.code.code(), item.message });
                }
            }
            ctx.printErr("Hint: run 'flo validate workflow -f {s}' for full diagnostics\n", .{file_path});
            return error.CommandFailed;
        }
    }

    // Extract workflow name from definition for key-based routing.
    // The server still validates the full definition; this just enables
    // the Acceptor to hash-route the connection to the correct shard.
    const wf_name = extractWorkflowName(definition) orelse "";

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.create(&client, namespace, wf_name, definition) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    if (result.asRawData()) |data| {
        ctx.print("Created workflow: {s}\n", .{data});
    } else {
        ctx.print("Workflow created successfully\n", .{});
    }
}

fn runStart(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name") orelse {
        ctx.printErr("Error: workflow name is required\n", .{});
        return error.CommandFailed;
    };
    const input = ctx.getPositional("input") orelse "{}";

    const version = ctx.getString("version") orelse "latest";
    const idempotency_key = ctx.getString("idempotency-key");
    const run_id = ctx.getString("run-id");
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    // Validate idempotency_key is not empty string
    const idem_key: ?[]const u8 = if (idempotency_key) |k| if (k.len > 0) k else null else null;
    const rid: ?[]const u8 = if (run_id) |r| if (r.len > 0) r else null else null;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.start(&client, namespace, name, version, input, idem_key, rid) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    if (result.asRawData()) |data| {
        ctx.print("Started workflow run: {s}\n", .{data});
    } else {
        ctx.print("Workflow started\n", .{});
    }
}

fn runSignal(ctx: *commander.Context) commander.Error!void {
    const run_id = ctx.getPositional("run_id") orelse {
        ctx.printErr("Error: run_id is required\n", .{});
        return error.CommandFailed;
    };

    const signal_type = ctx.getString("type") orelse "";
    if (signal_type.len == 0) {
        ctx.printErr("Error: --type is required\n", .{});
        return error.CommandFailed;
    }

    const payload = ctx.getString("payload");
    const payload_val: ?[]const u8 = if (payload) |p| if (p.len > 0) p else null else null;

    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.signal(&client, namespace, run_id, signal_type, payload_val) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("Signal '{s}' sent to workflow {s}\n", .{ signal_type, run_id });
}

fn runStatus(ctx: *commander.Context) commander.Error!void {
    const run_id = ctx.getPositional("run_id") orelse {
        ctx.printErr("Error: run_id is required\n", .{});
        return error.CommandFailed;
    };

    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.status(&client, namespace, run_id) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.printErr("Workflow run not found: {s}\n", .{run_id});
        return error.CommandFailed;
    }

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    if (result.asRawData()) |data| {
        printWorkflowRunStatus(ctx, run_id, data);
    }
}

fn printWorkflowRunStatus(ctx: *commander.Context, run_id: []const u8, data: []const u8) void {
    var off: usize = 0;

    // skip run_id field (already have it)
    if (off + 2 > data.len) return;
    const rid_len = std.mem.readInt(u16, data[off..][0..2], .little);
    off += 2 + rid_len;

    // workflow name
    if (off + 2 > data.len) return;
    const wf_len = std.mem.readInt(u16, data[off..][0..2], .little);
    off += 2;
    if (off + wf_len > data.len) return;
    const workflow = data[off .. off + wf_len];
    off += wf_len;

    // version
    if (off + 2 > data.len) return;
    const ver_len = std.mem.readInt(u16, data[off..][0..2], .little);
    off += 2;
    if (off + ver_len > data.len) return;
    const version = data[off .. off + ver_len];
    off += ver_len;

    // status
    if (off >= data.len) return;
    const status_str: []const u8 = switch (data[off]) {
        0 => "pending",
        1 => "running",
        2 => "waiting",
        3 => "completed",
        4 => "failed",
        5 => "cancelled",
        6 => "timed_out",
        else => "unknown",
    };
    off += 1;

    // current_step
    if (off + 2 > data.len) return;
    const step_len = std.mem.readInt(u16, data[off..][0..2], .little);
    off += 2;
    if (off + step_len > data.len) return;
    const current_step = data[off .. off + step_len];
    off += step_len;

    // input (u32-prefixed)
    if (off + 4 > data.len) return;
    const input_len = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    if (off + input_len > data.len) return;
    const input = data[off .. off + input_len];
    off += input_len;

    // created_at
    if (off + 8 > data.len) return;
    const created_at = std.mem.readInt(i64, data[off..][0..8], .little);
    off += 8;

    // started_at (optional)
    var started_at: ?i64 = null;
    if (off < data.len) {
        if (data[off] == 1 and off + 9 <= data.len) {
            off += 1;
            started_at = std.mem.readInt(i64, data[off..][0..8], .little);
            off += 8;
        } else {
            off += 1;
        }
    }

    // completed_at (optional)
    var completed_at: ?i64 = null;
    if (off < data.len) {
        if (data[off] == 1 and off + 9 <= data.len) {
            off += 1;
            completed_at = std.mem.readInt(i64, data[off..][0..8], .little);
            off += 8;
        } else {
            off += 1;
        }
    }

    // wait_signal (optional)
    var wait_signal: ?[]const u8 = null;
    if (off < data.len and data[off] == 1) {
        off += 1;
        if (off + 2 <= data.len) {
            const wsig_len = std.mem.readInt(u16, data[off..][0..2], .little);
            off += 2;
            if (off + wsig_len <= data.len) {
                wait_signal = data[off .. off + wsig_len];
            }
        }
    }

    ctx.print("Run:      {s}\n", .{run_id});
    ctx.print("Workflow: {s} (v{s})\n", .{ workflow, version });
    ctx.print("Status:   {s}\n", .{status_str});
    ctx.print("Step:     {s}\n", .{current_step});
    ctx.print("Input:    {s}\n", .{input});
    ctx.print("Created:  {d}ms\n", .{created_at});
    if (started_at) |v| ctx.print("Started:  {d}ms\n", .{v});
    if (completed_at) |v| ctx.print("Completed:{d}ms\n", .{v});
    if (wait_signal) |sig| ctx.print("Waiting:  signal={s}\n", .{sig});
}

fn runHistory(ctx: *commander.Context) commander.Error!void {
    const run_id = ctx.getPositional("run_id") orelse {
        ctx.printErr("Error: run_id is required\n", .{});
        return error.CommandFailed;
    };

    const limit = ctx.getUint("limit") orelse 100;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.history(&client, namespace, run_id, @intCast(limit)) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.printErr("Workflow run not found: {s}\n", .{run_id});
        return error.CommandFailed;
    }

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("History for run: {s}\n", .{run_id});
    if (result.asRawData()) |data| {
        output.printWireList(ctx, data, "(no events)", &.{
            .{ .field = "type", .header = "TYPE", .field_type = .str_u16, .alignment = .left },
            .{ .field = "detail", .header = "DETAIL", .field_type = .str_u16, .alignment = .left },
            .{ .field = "timestamp", .header = "", .field_type = .int_i64 },
        });
    } else {
        ctx.print("(no events)\n", .{});
    }
}

fn runListRuns(ctx: *commander.Context) commander.Error!void {
    const wf_flag = ctx.getString("workflow");
    const positional = ctx.getPositional("workflow_name");
    const name: []const u8 = blk: {
        if (wf_flag) |w| {
            if (w.len > 0) break :blk w;
        }
        if (positional) |p| {
            if (p.len > 0) break :blk p;
        }
        break :blk "";
    };

    const status_filter = ctx.getString("status");
    const status_val: ?[]const u8 = if (status_filter) |s| if (s.len > 0) s else null else null;

    const search_filter = ctx.getString("search");
    const search_val: ?[]const u8 = if (search_filter) |s| if (s.len > 0) s else null else null;

    // Require at least one filter
    if (name.len == 0 and search_val == null and status_val == null) {
        ctx.printErr("Error: specify --workflow, --status, or --search to list runs\n", .{});
        return error.CommandFailed;
    }

    const limit = ctx.getUint("limit") orelse 100;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.listRuns(&client, namespace, name, @intCast(limit), status_val, null, search_val) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    if (name.len > 0) {
        ctx.print("Runs for workflow: {s}\n", .{name});
    } else {
        ctx.print("Runs across all workflows:\n", .{});
    }
    if (result.asRawData()) |data| {
        output.printWireList(ctx, data, "(no runs)", &.{
            .{ .field = "run_id", .header = "RUN ID", .field_type = .str_u16, .alignment = .left },
            .{ .field = "workflow", .header = "WORKFLOW", .field_type = .str_u16, .alignment = .left },
            .{ .field = "status", .header = "STATUS", .field_type = .str_u16, .alignment = .left },
            .{ .field = "created_at", .header = "", .field_type = .int_i64 },
        });
    } else {
        ctx.print("(no runs)\n", .{});
    }
}

fn runCancel(ctx: *commander.Context) commander.Error!void {
    const run_id = ctx.getPositional("run_id") orelse {
        ctx.printErr("Error: run_id is required\n", .{});
        return error.CommandFailed;
    };

    const reason = ctx.getString("reason");
    const reason_val: ?[]const u8 = if (reason) |r| if (r.len > 0) r else null else null;

    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.cancel(&client, namespace, run_id, reason_val) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.printErr("Workflow run not found: {s}\n", .{run_id});
        return error.CommandFailed;
    }

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("Workflow {s} cancelled\n", .{run_id});
}

fn runGetDefinition(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name") orelse {
        ctx.printErr("Error: workflow name is required\n", .{});
        return error.CommandFailed;
    };

    const version = ctx.getString("version");
    const version_val: ?[]const u8 = if (version) |v| if (v.len > 0) v else null else null;

    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.getDefinition(&client, namespace, name, version_val) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.printErr("Workflow not found: {s}\n", .{name});
        return error.CommandFailed;
    }

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    if (result.asRawData()) |data| {
        if (data.len > 0) {
            ctx.print("{s}\n", .{data});
        } else {
            ctx.print("(no definition)\n", .{});
        }
    } else {
        ctx.print("(no definition)\n", .{});
    }
}

fn runDisable(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name") orelse {
        ctx.printErr("Error: workflow name is required\n", .{});
        return error.CommandFailed;
    };

    const version = ctx.getString("version");
    const version_val: ?[]const u8 = if (version) |v| if (v.len > 0) v else null else null;

    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.disable(&client, namespace, name, version_val) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    if (version_val) |v| {
        ctx.print("Workflow '{s}' v{s} disabled\n", .{ name, v });
    } else {
        ctx.print("Workflow '{s}' disabled\n", .{name});
    }
}

fn runEnable(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name") orelse {
        ctx.printErr("Error: workflow name is required\n", .{});
        return error.CommandFailed;
    };

    const version = ctx.getString("version");
    const version_val: ?[]const u8 = if (version) |v| if (v.len > 0) v else null else null;

    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.enable(&client, namespace, name, version_val) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    if (version_val) |v| {
        ctx.print("Workflow '{s}' v{s} enabled\n", .{ name, v });
    } else {
        ctx.print("Workflow '{s}' enabled\n", .{name});
    }
}

fn runListDefinitions(ctx: *commander.Context) commander.Error!void {
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.listDefinitions(&client, namespace) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    const data = result.asRawData() orelse {
        ctx.print("(no workflows)\n", .{});
        return;
    };

    output.printWireList(ctx, data, "(no workflows)", &.{
        .{ .field = "name", .header = "NAME", .field_type = .str_u16, .alignment = .left },
        .{ .field = "version", .header = "VERSION", .field_type = .str_u16, .alignment = .left },
        .{ .field = "created_at", .header = "", .field_type = .int_i64 },
    });
}

// =============================================================================
// Helpers
// =============================================================================

/// Extract the "name" field from a workflow definition (JSON or YAML).
///
/// Returns a slice into `content` — no allocation needed.  Handles:
///   JSON:  "name": "my-workflow"      (anywhere on a line)
///   YAML:  name: my-workflow          (at start of line only)
///
/// Best-effort: if extraction fails, the caller should send key="" and
/// let the server parse the full definition as fallback.
fn extractWorkflowName(content: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i + 4 <= content.len) {
        const pos = std.mem.indexOfPos(u8, content, i, "name") orelse break;

        // Must be a top-level key: preceded by " (JSON) or newline/start (YAML)
        if (pos > 0) {
            const before = content[pos - 1];
            if (before != '"' and before != '\n' and before != '\r') {
                i = pos + 4;
                continue;
            }
        }

        var j = pos + 4;

        // JSON key: skip closing quote of "name"
        if (j < content.len and content[j] == '"') j += 1;

        // Skip whitespace before colon
        while (j < content.len and (content[j] == ' ' or content[j] == '\t')) : (j += 1) {}

        // Must have colon separator
        if (j >= content.len or content[j] != ':') {
            i = pos + 4;
            continue;
        }
        j += 1;

        // Skip whitespace after colon
        while (j < content.len and (content[j] == ' ' or content[j] == '\t')) : (j += 1) {}
        if (j >= content.len) break;

        // Quoted string value
        if (content[j] == '"') {
            const start = j + 1;
            if (std.mem.indexOfScalarPos(u8, content, start, '"')) |end| {
                if (end > start) return content[start..end];
            }
        } else if (content[j] != '\n' and content[j] != '\r' and content[j] != '{' and content[j] != '[') {
            // Unquoted YAML value
            const start = j;
            var end = j;
            while (end < content.len and content[end] != '\n' and content[end] != '\r') : (end += 1) {}
            // Trim trailing whitespace and commas
            while (end > start and (content[end - 1] == ' ' or content[end - 1] == '\t' or content[end - 1] == ',')) : (end -= 1) {}
            if (end > start) return content[start..end];
        }

        i = pos + 4;
    }
    return null;
}
