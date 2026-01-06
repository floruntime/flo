//! Workflow commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo workflow create -f <definition.yaml>
//!   flo workflow start <name> [--version <ver>] [--input <json>] [--idempotency-key <key>]
//!   flo workflow signal <run_id> --type <signal_type> [--payload <json>]
//!   flo workflow status <run_id>
//!   flo workflow history <run_id> [--limit <n>]
//!   flo workflow list-runs <name> [--status <status>] [--limit <n>]
//!   flo workflow cancel <run_id> [--reason <reason>]
//!   flo workflow definition <name> [--version <ver>]
//!   flo workflow disable <name> [--version <ver>]
//!   flo workflow enable <name> [--version <ver>]

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;

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
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
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
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
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
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runSignal)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("status")
                .about("Get workflow run status")
                .aliases(&.{"get"})
                .arg("run_id", "Workflow run ID")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runStatus)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("history")
                .about("Get workflow run history (events)")
                .arg("run_id", "Workflow run ID")
                .uintFlag("limit", 'l', 100, "Maximum events to show")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runHistory)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list-runs")
                .about("List workflow runs")
                .aliases(&.{ "ls", "runs" })
                .arg("name", "Workflow name")
                .stringFlag("status", 's', "", "Filter by status (running, completed, failed, cancelled)")
                .uintFlag("limit", 'l', 100, "Maximum runs to show")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runListRuns)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("cancel")
                .about("Cancel a running workflow")
                .arg("run_id", "Workflow run ID")
                .stringFlag("reason", 'r', "", "Cancellation reason")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runCancel)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("definition")
                .about("Get workflow definition")
                .aliases(&.{ "def", "show" })
                .arg("name", "Workflow name")
                .stringFlag("version", 'v', "", "Specific version (default: latest)")
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
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
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
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
                .stringFlag("namespace", 'n', "default", "Namespace to use")
                .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                .action(wrapHandler(runEnable)),
        )
        .build();
}

// =============================================================================
// Command Handlers
// =============================================================================

/// Get endpoint from flags or config
fn getEndpoint(ctx: *commander.Context) []const u8 {
    if (ctx.getString("endpoint")) |ep| {
        if (ep.len > 0) return ep;
    }
    return "127.0.0.1:9000";
}

fn runCreate(ctx: *commander.Context) commander.Error!void {
    const file_path = ctx.getString("file") orelse "";
    if (file_path.len == 0) {
        ctx.printErr("Error: --file is required\n", .{});
        return error.CommandFailed;
    }

    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

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

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.create(&client, namespace, definition) catch |err| {
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
    const endpoint = getEndpoint(ctx);

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
    const endpoint = getEndpoint(ctx);

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
    const endpoint = getEndpoint(ctx);

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

    ctx.print("Run: {s}\n", .{run_id});
    if (result.asRawData()) |data| {
        ctx.print("{s}\n", .{data});
    }
}

fn runHistory(ctx: *commander.Context) commander.Error!void {
    const run_id = ctx.getPositional("run_id") orelse {
        ctx.printErr("Error: run_id is required\n", .{});
        return error.CommandFailed;
    };

    const limit = ctx.getUint("limit") orelse 100;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

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
        if (data.len > 0) {
            ctx.print("{s}\n", .{data});
        } else {
            ctx.print("(no events)\n", .{});
        }
    } else {
        ctx.print("(no events)\n", .{});
    }
}

fn runListRuns(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name") orelse {
        ctx.printErr("Error: workflow name is required\n", .{});
        return error.CommandFailed;
    };

    const status_filter = ctx.getString("status");
    const status_val: ?[]const u8 = if (status_filter) |s| if (s.len > 0) s else null else null;

    const limit = ctx.getUint("limit") orelse 100;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.workflow.listRuns(&client, namespace, name, @intCast(limit), status_val, null) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("Runs for workflow: {s}\n", .{name});
    if (result.asRawData()) |data| {
        if (data.len > 0) {
            ctx.print("{s}\n", .{data});
        } else {
            ctx.print("(no runs)\n", .{});
        }
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
    const endpoint = getEndpoint(ctx);

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
    const endpoint = getEndpoint(ctx);

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
    const endpoint = getEndpoint(ctx);

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
    const endpoint = getEndpoint(ctx);

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
