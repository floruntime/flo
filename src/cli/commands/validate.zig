//! Validate commands for Flo CLI
//!
//! Offline YAML/JSON linting for workflow and processing definitions.
//! No server connection required — runs the same parsers used server-side
//! and performs semantic validation on top.
//!
//! Usage:
//!   flo validate workflow -f <definition.yaml>
//!   flo validate processing -f <definition.yaml>

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const commander = @import("../commander/mod.zig");
const output = @import("../output.zig");
const outcome = @import("../outcome.zig");

const wf_parser = @import("../../workflow/parser.zig");
const wf_definition = @import("../../workflow/definition.zig");
const wf_validator = @import("../../workflow/validator.zig");
const proc_parser = @import("../../processing/parser.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the validate command tree
pub fn createValidateCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("validate")
        .about("Validate YAML definitions offline (no server needed)")
        .group("Other Commands")
        .longAbout(
            \\Lint and validate workflow or processing YAML/JSON definitions
            \\without connecting to a Flo server. Runs the same parsers used
            \\server-side and checks for semantic errors.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("workflow")
                .about("Validate a workflow definition file")
                .examples(&.{
                    "flo validate workflow -f order-processing.yaml",
                    "flo validate workflow --file ./workflows/payment.yaml",
                })
                .stringFlag("file", 'f', "", "YAML/JSON definition file")
                .action(wrapHandler(runValidateWorkflow)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("processing")
                .about("Validate a processing pipeline definition file")
                .examples(&.{
                    "flo validate processing -f pipeline.yaml",
                    "flo validate processing --file ./jobs/etl.yaml",
                })
                .stringFlag("file", 'f', "", "YAML/JSON definition file")
                .action(wrapHandler(runValidateProcessing)),
        )
        .build();
}

// =============================================================================
// Workflow Validation
// =============================================================================

fn runValidateWorkflow(ctx: *commander.Context) commander.Error!void {
    const file_path = ctx.getString("file") orelse "";
    if (file_path.len == 0) {
        return outcome.usage(ctx, "--file is required", .{});
    }

    const content = readFile(ctx, file_path) orelse return error.Usage;
    defer ctx.allocator.free(content);

    // Phase 1: Parse (with pre-flight checks for better diagnostics)
    var diag: wf_parser.Diagnostic = .{};
    var def = wf_parser.parseWorkflow(ctx.allocator, content, &diag) catch |err| {
        if (err == error.OutOfMemory) {
            ctx.printErr("FAIL  out of memory\n", .{});
            return error.OutOfMemory;
        }
        ctx.printErr("FAIL  {s}\n", .{diag.message()});
        return error.Usage;
    };
    defer def.deinit(ctx.allocator);

    ctx.print("OK    Parsed workflow '{s}' v{s}\n", .{ def.name, def.version });

    // Phase 2: Run the canonical server-side validator
    var validation = wf_validator.validateWorkflow(ctx.allocator, &def) catch {
        ctx.printErr("FAIL  Internal validation error\n", .{});
        return error.Refused;
    };
    defer validation.deinit();

    var errors: usize = 0;
    var warnings: usize = 0;

    // Report all validator findings with error codes
    for (validation.items()) |item| {
        reportItem(ctx, item);
        if (item.severity == .@"error") {
            errors += 1;
        } else {
            warnings += 1;
        }
    }

    // Summary
    ctx.print("\n", .{});
    if (errors > 0) {
        ctx.printErr("FAILED: {d} error(s), {d} warning(s)\n", .{ errors, warnings });
        return error.Usage;
    } else if (warnings > 0) {
        ctx.print("PASSED with {d} warning(s)\n", .{warnings});
    } else {
        ctx.print("PASSED: workflow definition is valid\n", .{});
    }
}

fn runValidateProcessing(ctx: *commander.Context) commander.Error!void {
    const file_path = ctx.getString("file") orelse "";
    if (file_path.len == 0) {
        return outcome.usage(ctx, "--file is required", .{});
    }

    const content = readFile(ctx, file_path) orelse return error.Usage;
    defer ctx.allocator.free(content);

    // Phase 1: Parse
    var diag: proc_parser.Diagnostic = .{};
    var def = proc_parser.parseJobDefinition(ctx.allocator, content, &diag) catch |err| {
        if (err == error.OutOfMemory) {
            ctx.printErr("FAIL  out of memory\n", .{});
            return error.OutOfMemory;
        }
        ctx.printErr("FAIL  {s}\n", .{diag.message()});
        return error.Usage;
    };
    defer def.deinit(ctx.allocator);

    ctx.print("OK    Parsed processing job '{s}'\n", .{def.name});

    // Phase 2: Semantic validation
    var errors: usize = 0;
    var warnings: usize = 0;

    // Must have at least one source
    if (def.sources.items.len == 0) {
        ctx.printErr("ERR   No sources defined (at least one source is required)\n", .{});
        errors += 1;
    }

    // Must have at least one sink
    if (def.sinks.items.len == 0) {
        ctx.printErr("ERR   No sinks defined (at least one sink is required)\n", .{});
        errors += 1;
    }

    // Check parallelism
    if (def.parallelism == 0) {
        ctx.printErr("ERR   Parallelism must be >= 1\n", .{});
        errors += 1;
    }

    // Check source names are unique
    {
        var seen = std.StringHashMap(void).init(ctx.allocator);
        defer seen.deinit();
        for (def.sources.items) |src| {
            if (seen.contains(src.name)) {
                ctx.printErr("ERR   Duplicate source name '{s}'\n", .{src.name});
                errors += 1;
            } else {
                seen.put(src.name, {}) catch {};
            }
        }
    }

    // Check sink names are unique
    {
        var seen = std.StringHashMap(void).init(ctx.allocator);
        defer seen.deinit();
        for (def.sinks.items) |sink| {
            if (seen.contains(sink.name)) {
                ctx.printErr("ERR   Duplicate sink name '{s}'\n", .{sink.name});
                errors += 1;
            } else {
                seen.put(sink.name, {}) catch {};
            }
        }
    }

    // Check operator names are unique
    {
        var seen = std.StringHashMap(void).init(ctx.allocator);
        defer seen.deinit();
        for (def.operators.items) |op| {
            if (seen.contains(op.name)) {
                ctx.printErr("ERR   Duplicate operator name '{s}'\n", .{op.name});
                errors += 1;
            } else {
                seen.put(op.name, {}) catch {};
            }
        }
    }

    // Validate sources have stream/ts names
    for (def.sources.items) |src| {
        switch (src.kind) {
            .stream => {
                if (src.stream.len == 0) {
                    ctx.printErr("ERR   Source '{s}': stream source missing stream name\n", .{src.name});
                    errors += 1;
                }
            },
            .ts => {
                if (src.ts_measurement.len == 0) {
                    ctx.printErr("ERR   Source '{s}': ts source missing measurement name\n", .{src.name});
                    errors += 1;
                }
            },
        }
    }

    // Validate sinks have target names
    for (def.sinks.items) |sink| {
        switch (sink.kind) {
            .stream, .queue => {
                if (sink.target.len == 0) {
                    ctx.printErr("ERR   Sink '{s}': {s} sink missing target name\n", .{ sink.name, sink.kind.toStr() });
                    errors += 1;
                }
            },
            .kv, .ts => {
                // KV/TS sinks valid without target
            },
        }
    }

    // Validate operators have types
    for (def.operators.items) |op| {
        if (op.type_name.len == 0) {
            ctx.printErr("ERR   Operator '{s}': missing type\n", .{op.name});
            errors += 1;
        }
    }

    // Warn if no operators defined
    if (def.operators.items.len == 0) {
        ctx.printErr("WARN  No operators defined — data will pass through unchanged\n", .{});
        warnings += 1;
    }

    // Print summary info
    ctx.print("      Sources: {d}, Sinks: {d}, Operators: {d}, Parallelism: {d}\n", .{
        def.sources.items.len,
        def.sinks.items.len,
        def.operators.items.len,
        def.parallelism,
    });

    // Summary
    ctx.print("\n", .{});
    if (errors > 0) {
        ctx.printErr("FAILED: {d} error(s), {d} warning(s)\n", .{ errors, warnings });
        return error.Usage;
    } else if (warnings > 0) {
        ctx.print("PASSED with {d} warning(s)\n", .{warnings});
    } else {
        ctx.print("PASSED: processing definition is valid\n", .{});
    }
}

fn reportItem(ctx: *commander.Context, item: anytype) void {
    const prefix: []const u8 = if (item.severity == .@"error") "ERR  " else "WARN ";
    if (item.location) |loc| {
        ctx.printErr("{s} [{s}] {s} (at '{s}')\n", .{ prefix, item.code.code(), item.message, loc });
    } else {
        ctx.printErr("{s} [{s}] {s}\n", .{ prefix, item.code.code(), item.message });
    }
}

fn readFile(ctx: *commander.Context, file_path: []const u8) ?[]u8 {
    const file = @import("stdx").fs.openFile(file_path, .{}) catch |err| {
        ctx.printErr("Failed to open file '{s}': {}\n", .{ file_path, err });
        return null;
    };
    defer @import("stdx").fs.closeFile(file);

    return @import("stdx").fs.readToEndAlloc(file, ctx.allocator, 4 * 1024 * 1024) catch |err| {
        ctx.printErr("Failed to read file: {}\n", .{err});
        return null;
    };
}
