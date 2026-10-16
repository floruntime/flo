//! Processing commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo processing submit <file> [--namespace <ns>]
//!   flo processing stop <job_id> [--namespace <ns>]
//!   flo processing cancel <job_id> [--namespace <ns>]
//!   flo processing status <job_id> [--namespace <ns>]
//!   flo processing list [--limit <n>] [--namespace <ns>]

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const outcome = @import("../outcome.zig");
const wire = @import("../../util/wire.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const cli_config = @import("../config.zig");
const output = @import("../output.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the processing command tree
pub fn createProcessingCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("processing")
        .about("Stream processing operations")
        .group("Data Commands")
        .longAbout(
            \\Manage stream processing jobs.
            \\
            \\Submit YAML job definitions, check their status, list them, and
            \\stop or cancel them.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("submit")
                .about("Submit a processing job")
                .examples(&.{
                    "flo processing submit job.yaml",
                    "flo processing submit pipeline.yaml --namespace prod",
                })
                .arg("file", "YAML job definition file")
                .action(wrapHandler(runSubmit)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("stop")
                .about("Gracefully stop a processing job")
                .examples(&.{
                    "flo processing stop job-123",
                })
                .arg("job_id", "Job ID to stop")
                .action(wrapHandler(runStop)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("cancel")
                .about("Force cancel a processing job")
                .examples(&.{
                    "flo processing cancel job-123",
                })
                .arg("job_id", "Job ID to cancel")
                .action(wrapHandler(runCancel)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("status")
                .about("Get processing job status")
                .examples(&.{
                    "flo processing status job-123",
                })
                .arg("job_id", "Job ID to query")
                .action(wrapHandler(runStatus)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List processing jobs")
                .aliases(&.{"ls"})
                .examples(&.{
                    "flo processing list",
                    "flo processing list --limit 10",
                })
                .uintFlag("limit", 'l', 100, "Maximum jobs to show")
                .action(wrapHandler(runList)),
        )
        .build();
}

// =============================================================================
// Command Handlers
// =============================================================================

fn runSubmit(ctx: *commander.Context) commander.Error!void {
    const file_path = ctx.getPositional("file").?;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    // Read the YAML file
    const yaml = @import("stdx").fs.readFileAlloc(ctx.allocator, file_path, 1024 * 1024) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return outcome.usage(ctx, "failed to read file '{s}': {}", .{ file_path, err });
    };
    defer ctx.allocator.free(yaml);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.processing.submit(&client, namespace, yaml) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    if (result.asRawData()) |job_id| {
        ctx.print("Job submitted: {s}\n", .{job_id});
    } else {
        ctx.print("Job submitted successfully\n", .{});
    }
}

fn runStop(ctx: *commander.Context) commander.Error!void {
    const job_id = ctx.getPositional("job_id").?;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.processing.stop(&client, namespace, job_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        return outcome.notFound(ctx, "Job not found: {s}", .{job_id});
    }

    try outcome.check(ctx, result);

    ctx.print("Job stopped: {s}\n", .{job_id});
}

fn runCancel(ctx: *commander.Context) commander.Error!void {
    const job_id = ctx.getPositional("job_id").?;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.processing.cancel(&client, namespace, job_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        return outcome.notFound(ctx, "Job not found: {s}", .{job_id});
    }

    try outcome.check(ctx, result);

    ctx.print("Job cancelled: {s}\n", .{job_id});
}

fn runStatus(ctx: *commander.Context) commander.Error!void {
    const job_id = ctx.getPositional("job_id").?;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.processing.status(&client, namespace, job_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        return outcome.notFound(ctx, "Job not found: {s}", .{job_id});
    }

    try outcome.check(ctx, result);

    if (result.asRawData()) |data| {
        try printProcessingJobStatus(ctx, data);
    }
}

fn printProcessingJobStatus(ctx: *commander.Context, data: []const u8) commander.Error!void {
    var r = wire.WireReader.init(data);
    const what = "processing job status";
    const job_id = r.readLengthPrefixed(u16) orelse return outcome.malformed(ctx, what);
    const name = r.readLengthPrefixed(u16) orelse return outcome.malformed(ctx, what);
    const status_str: []const u8 = switch (r.readU8() orelse return outcome.malformed(ctx, what)) {
        0 => "RUNNING",
        1 => "STOPPED",
        2 => "CANCELLED",
        3 => "FAILED",
        4 => "COMPLETED",
        else => "UNKNOWN",
    };
    const batch_size = r.readU32() orelse return outcome.malformed(ctx, what);
    const records = r.readU64() orelse return outcome.malformed(ctx, what);
    const created_at = r.readI64() orelse return outcome.malformed(ctx, what);

    ctx.print("Job:        {s}\n", .{job_id});
    ctx.print("Name:       {s}\n", .{name});
    ctx.print("Status:     {s}\n", .{status_str});
    ctx.print("Batch size: {d}\n", .{batch_size});
    ctx.print("Processed:  {d} records\n", .{records});
    ctx.print("Created:    {d}ms\n", .{created_at});
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    const limit = ctx.getUint("limit") orelse 100;
    var result = client_mod.processing.list(&client, namespace, limit, null) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    const data = result.asRawData() orelse {
        output.printEmptyList(ctx, "(no processing jobs)");
        return;
    };

    output.printWireList(ctx, data, "(no processing jobs)", &.{
        .{ .field = "name", .header = "NAME", .field_type = .str_u16, .alignment = .left },
        .{ .field = "job_id", .header = "JOB ID", .field_type = .str_u16, .alignment = .left },
        .{ .field = "status", .header = "STATUS", .field_type = .str_u16, .alignment = .left },
        .{ .field = "created_at_ms", .header = "", .field_type = .int_i64 },
    }) catch return outcome.malformed(ctx, "list");
}
