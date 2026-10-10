//! Processing commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo processing submit <file> [--namespace <ns>]
//!   flo processing stop <job_id> [--namespace <ns>]
//!   flo processing cancel <job_id> [--namespace <ns>]
//!   flo processing status <job_id> [--namespace <ns>]
//!   flo processing list [--limit <n>] [--namespace <ns>]
//!   flo processing savepoint <job_id> [--namespace <ns>]
//!   flo processing restore <job_id> <savepoint_id> [--namespace <ns>]
//!   flo processing rescale <job_id> <parallelism> [--namespace <ns>]

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
            \\Manage stream processing jobs (Flink-inspired).
            \\
            \\Submit YAML job definitions, monitor status, collect metrics,
            \\create savepoints, and rescale running jobs.
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
        .subcommand(
            commander.newBuilder(allocator)
                .name("savepoint")
                .about("Trigger a savepoint")
                .examples(&.{
                    "flo processing savepoint job-123",
                })
                .arg("job_id", "Job ID to savepoint")
                .action(wrapHandler(runSavepoint)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("restore")
                .about("Restore from a savepoint")
                .examples(&.{
                    "flo processing restore job-123 sp-456",
                })
                .arg("job_id", "Job ID to restore")
                .arg("savepoint_id", "Savepoint ID to restore from")
                .action(wrapHandler(runRestore)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("rescale")
                .about("Rescale job parallelism")
                .examples(&.{
                    "flo processing rescale job-123 4",
                })
                .arg("job_id", "Job ID to rescale")
                .arg("parallelism", "New parallelism level")
                .action(wrapHandler(runRescale)),
        )
        .build();
}

// =============================================================================
// Command Handlers
// =============================================================================

fn runSubmit(ctx: *commander.Context) commander.Error!void {
    const file_path = ctx.getPositional("file").?;
    const namespace = ctx.getString("namespace") orelse "default";
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
    const namespace = ctx.getString("namespace") orelse "default";
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
    const namespace = ctx.getString("namespace") orelse "default";
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
    const namespace = ctx.getString("namespace") orelse "default";
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
    const parallelism = r.readU32() orelse return outcome.malformed(ctx, what);
    const batch_size = r.readU32() orelse return outcome.malformed(ctx, what);
    const records = r.readU64() orelse return outcome.malformed(ctx, what);
    const created_at = r.readI64() orelse return outcome.malformed(ctx, what);

    ctx.print("Job:        {s}\n", .{job_id});
    ctx.print("Name:       {s}\n", .{name});
    ctx.print("Status:     {s}\n", .{status_str});
    ctx.print("Parallelism:{d}\n", .{parallelism});
    ctx.print("Batch size: {d}\n", .{batch_size});
    ctx.print("Processed:  {d} records\n", .{records});
    ctx.print("Created:    {d}ms\n", .{created_at});
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.processing.list(&client, namespace, null) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    const data = result.asRawData() orelse {
        ctx.print("(no processing jobs)\n", .{});
        return;
    };

    output.printWireList(ctx, data, "(no processing jobs)", &.{
        .{ .field = "name", .header = "NAME", .field_type = .str_u16, .alignment = .left },
        .{ .field = "job_id", .header = "JOB ID", .field_type = .str_u16, .alignment = .left },
        .{ .field = "status", .header = "STATUS", .field_type = .str_u16, .alignment = .left },
        .{ .field = "parallelism", .header = "", .field_type = .uint_u32 },
        .{ .field = "created_at_ms", .header = "", .field_type = .int_i64 },
    }) catch return outcome.malformed(ctx, "list");
}

fn runSavepoint(ctx: *commander.Context) commander.Error!void {
    const job_id = ctx.getPositional("job_id").?;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.processing.savepoint(&client, namespace, job_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        return outcome.notFound(ctx, "Job not found: {s}", .{job_id});
    }

    try outcome.check(ctx, result);

    if (result.asRawData()) |sp_id| {
        ctx.print("Savepoint created: {s}\n", .{sp_id});
    } else {
        ctx.print("Savepoint created\n", .{});
    }
}

fn runRestore(ctx: *commander.Context) commander.Error!void {
    const job_id = ctx.getPositional("job_id").?;
    const savepoint_id = ctx.getPositional("savepoint_id").?;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.processing.restore(&client, namespace, job_id, savepoint_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        return outcome.notFound(ctx, "job {s} or savepoint {s} not found", .{ job_id, savepoint_id });
    }

    try outcome.check(ctx, result);

    ctx.print("Job {s} restored from savepoint {s}\n", .{ job_id, savepoint_id });
}

fn runRescale(ctx: *commander.Context) commander.Error!void {
    const job_id = ctx.getPositional("job_id").?;
    const parallelism_str = ctx.getPositional("parallelism").?;
    const namespace = ctx.getString("namespace") orelse "default";
    const endpoint = cli_config.getEndpoint(ctx);

    const parallelism = std.fmt.parseInt(u32, parallelism_str, 10) catch {
        return outcome.usage(ctx, "invalid parallelism: {s} (must be a positive integer)", .{parallelism_str});
    };

    if (parallelism == 0) {
        return outcome.usage(ctx, "parallelism must be at least 1", .{});
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.processing.rescale(&client, namespace, job_id, parallelism) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        return outcome.notFound(ctx, "Job not found: {s}", .{job_id});
    }

    try outcome.check(ctx, result);

    ctx.print("Job {s} rescaled to parallelism {d}\n", .{ job_id, parallelism });
}
