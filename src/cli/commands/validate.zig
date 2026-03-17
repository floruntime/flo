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

const wf_parser = @import("../../workflow/parser.zig");
const wf_definition = @import("../../workflow/definition.zig");
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
        ctx.printErr("Error: --file is required\n", .{});
        return error.CommandFailed;
    }

    const content = readFile(ctx, file_path) orelse return error.CommandFailed;
    defer ctx.allocator.free(content);

    // Phase 1: Parse (with pre-flight checks for better diagnostics)
    var def = wf_parser.parseWorkflow(ctx.allocator, content) catch |err| {
        ctx.printErr("FAIL  {s}\n", .{workflowParseErrorString(err)});
        if (err == error.MissingRequiredField) {
            printWorkflowFieldHints(ctx, content);
        }
        return error.CommandFailed;
    };
    defer def.deinit(ctx.allocator);

    ctx.print("OK    Parsed workflow '{s}' v{s}\n", .{ def.name, def.version });

    // Phase 2: Semantic validation
    var errors: usize = 0;
    var warnings: usize = 0;

    // Collect valid step names
    var step_names = std.StringHashMap(void).init(ctx.allocator);
    defer step_names.deinit();
    for (def.steps) |step| {
        step_names.put(step.name, {}) catch {};
    }

    // Collect valid terminal names (built-in + custom)
    var terminal_names = std.StringHashMap(void).init(ctx.allocator);
    defer terminal_names.deinit();
    terminal_names.put("flo.Completed", {}) catch {};
    terminal_names.put("flo.Failed", {}) catch {};
    terminal_names.put("flo.Cancelled", {}) catch {};
    terminal_names.put("flo.TimedOut", {}) catch {};
    for (def.terminals) |t| {
        terminal_names.put(t.name, {}) catch {};
    }

    // Collect inline plan names
    var plan_names = std.StringHashMap(void).init(ctx.allocator);
    defer plan_names.deinit();
    for (def.plans) |p| {
        plan_names.put(p.name, {}) catch {};
    }

    // Check start step transitions
    errors += validateStepTransitions(ctx, "start", def.start, &step_names, &terminal_names);

    // Check each named step's transitions
    for (def.steps) |step| {
        errors += validateStepTransitions(ctx, step.name, step.step, &step_names, &terminal_names);
    }

    // Check that start step transitions reference at least one defined step or terminal
    const start_transitions = getStepTransitions(def.start);
    if (start_transitions.len == 0) {
        ctx.printErr("WARN  'start' step has no transitions\n", .{});
        warnings += 1;
    }

    // Check for unreachable steps (not referenced by any transition)
    for (def.steps) |step| {
        if (!isStepReachable(step.name, def.start, def.steps)) {
            ctx.printErr("WARN  Step '{s}' is not reachable from any transition\n", .{step.name});
            warnings += 1;
        }
    }

    // Check that @plan/ references point to defined inline plans
    checkPlanReferences(ctx, "start", def.start, &plan_names, &errors);
    for (def.steps) |step| {
        checkPlanReferences(ctx, step.name, step.step, &plan_names, &errors);
    }

    // Check custom terminals have valid status mappings
    for (def.terminals) |t| {
        const status = t.status;
        if (!status.isTerminal()) {
            ctx.printErr("ERR   Terminal '{s}' maps to non-terminal status '{s}'\n", .{ t.name, status.toString() });
            errors += 1;
        }
    }

    // Check schedule validity
    if (def.schedule) |sched| {
        if (sched.cron_expr == null and sched.interval_ms == null) {
            ctx.printErr("ERR   Schedule defined but missing both 'cron' and 'interval'\n", .{});
            errors += 1;
        }
        if (sched.cron_expr != null and sched.interval_ms != null) {
            ctx.printErr("ERR   Schedule has both 'cron' and 'interval' (mutually exclusive)\n", .{});
            errors += 1;
        }
    }

    // Summary
    ctx.print("\n", .{});
    if (errors > 0) {
        ctx.printErr("FAILED: {d} error(s), {d} warning(s)\n", .{ errors, warnings });
        return error.CommandFailed;
    } else if (warnings > 0) {
        ctx.print("PASSED with {d} warning(s)\n", .{warnings});
    } else {
        ctx.print("PASSED: workflow definition is valid\n", .{});
    }
}

fn validateStepTransitions(
    ctx: *commander.Context,
    step_name: []const u8,
    step: wf_definition.Step,
    step_names: *std.StringHashMap(void),
    terminal_names: *std.StringHashMap(void),
) usize {
    var err_count: usize = 0;
    const transitions = getStepTransitions(step);
    for (transitions) |t| {
        // Target must be a known step name or a terminal
        if (!step_names.contains(t.target) and !terminal_names.contains(t.target)) {
            ctx.printErr("ERR   Step '{s}': transition outcome '{s}' -> '{s}' references unknown step or terminal\n", .{ step_name, t.outcome, t.target });
            err_count += 1;
        }
    }
    return err_count;
}

fn getStepTransitions(step: wf_definition.Step) []const wf_definition.Transition {
    return switch (step) {
        .run => |r| r.transitions,
        .wait_for_signal => |w| w.transitions,
    };
}

fn isStepReachable(name: []const u8, start: wf_definition.Step, steps: []const wf_definition.NamedStep) bool {
    // Check start step transitions
    for (getStepTransitions(start)) |t| {
        if (mem.eql(u8, t.target, name)) return true;
    }
    // Check all named steps
    for (steps) |step| {
        for (getStepTransitions(step.step)) |t| {
            if (mem.eql(u8, t.target, name)) return true;
        }
    }
    return false;
}

fn checkPlanReferences(
    ctx: *commander.Context,
    step_name: []const u8,
    step: wf_definition.Step,
    plan_names: *std.StringHashMap(void),
    errors: *usize,
) void {
    switch (step) {
        .run => |r| {
            if (r.isPlan()) {
                const plan_key = r.targetName();
                if (!plan_names.contains(plan_key)) {
                    ctx.printErr("ERR   Step '{s}': references plan '{s}' which is not defined in 'plans:'\n", .{ step_name, r.target });
                    errors.* += 1;
                }
            }
        },
        .wait_for_signal => {},
    }
}

fn workflowParseErrorString(err: wf_parser.ParseError) []const u8 {
    return switch (err) {
        error.InvalidFormat => "invalid YAML/JSON format",
        error.MissingRequiredField => "missing required field",
        error.InvalidFieldType => "field has wrong type",
        error.InvalidKind => "kind must be 'Workflow'",
        error.InvalidIdempotencyMode => "idempotency must be 'none', 'optional', or 'required'",
        error.InvalidSelectionStrategy => "invalid selection strategy (use: static-order, round-robin, random, or health-weighted)",
        error.InvalidBackoffType => "invalid backoff type (use: constant, linear, or exponential)",
        error.InvalidSearchAttrType => "invalid search attribute type (use: string, number, or timestamp)",
        error.InvalidFallbackCondition => "invalid fallback condition",
        error.InvalidTrackingMode => "invalid tracking mode (use: sync or async)",
        error.DuplicateStepName => "duplicate step name",
        error.DuplicateExecutorName => "duplicate executor name in plan",
        error.DuplicatePlanName => "duplicate inline plan name",
        error.EmptyExecutors => "plan has empty executors list",
        error.OutOfMemory => "out of memory",
    };
}

/// When MissingRequiredField fires, scan the raw YAML to hint at what's actually missing.
fn printWorkflowFieldHints(ctx: *commander.Context, content: []const u8) void {
    // Check top-level required fields
    if (!yamlHasKey(content, "kind"))
        ctx.printErr("HINT  missing top-level 'kind: Workflow'\n", .{});
    if (!yamlHasKey(content, "name"))
        ctx.printErr("HINT  missing top-level 'name:'\n", .{});
    if (!yamlHasKey(content, "version"))
        ctx.printErr("HINT  missing top-level 'version:'\n", .{});
    if (!yamlHasKey(content, "start"))
        ctx.printErr("HINT  missing top-level 'start:' step\n", .{});

    // Check common executor mistakes inside plans
    if (yamlHasKey(content, "executors")) {
        const has_action_in_list = yamlHasArrayItemKey(content, "action");
        const has_run_in_list = yamlHasArrayItemKey(content, "run");
        const has_name_in_list = yamlHasArrayItemKey(content, "name");

        if (has_action_in_list and !has_run_in_list) {
            ctx.printErr("HINT  executor uses 'action:' — did you mean 'run:'? (executors need 'name:' and 'run:' fields)\n", .{});
        } else if (!has_run_in_list) {
            ctx.printErr("HINT  executor may be missing 'run:' field (executors need 'name:' and 'run:')\n", .{});
        }
        if (!has_name_in_list) {
            ctx.printErr("HINT  executor is missing 'name:' field\n", .{});
        }
    }

    // Check signal steps
    if (yamlHasKey(content, "waitForSignal")) {
        if (!yamlLineHasKey(content, "type:")) {
            ctx.printErr("HINT  waitForSignal step is missing 'type:'\n", .{});
        }
    }
}

/// Quick heuristic: does the YAML contain a line starting with this key at any indent?
fn yamlHasKey(content: []const u8, key: []const u8) bool {
    var iter = mem.splitScalar(u8, content, '\n');
    while (iter.next()) |line| {
        const trimmed = mem.trimLeft(u8, line, " \t");
        if (mem.startsWith(u8, trimmed, key)) return true;
    }
    return false;
}

/// Check if any line contains this exact token (e.g. "run:" not inside a comment)
fn yamlLineHasKey(content: []const u8, key: []const u8) bool {
    var iter = mem.splitScalar(u8, content, '\n');
    while (iter.next()) |line| {
        const trimmed = mem.trimLeft(u8, line, " \t-");
        if (trimmed.len == 0 or trimmed[0] == '#') continue;
        if (mem.startsWith(u8, trimmed, key)) return true;
    }
    return false;
}

/// Check if any YAML array item line (starting with "- ") contains "key:"
fn yamlHasArrayItemKey(content: []const u8, key: []const u8) bool {
    var iter = mem.splitScalar(u8, content, '\n');
    while (iter.next()) |line| {
        const trimmed = mem.trimLeft(u8, line, " \t");
        if (trimmed.len == 0 or trimmed[0] == '#') continue;
        // Must be an array item line
        if (!mem.startsWith(u8, trimmed, "- ")) continue;
        const item = mem.trimLeft(u8, trimmed[2..], " \t");
        if (mem.startsWith(u8, item, key) and item.len > key.len and item[key.len] == ':') return true;
    }
    return false;
}

// =============================================================================
// Processing Validation
// =============================================================================

fn runValidateProcessing(ctx: *commander.Context) commander.Error!void {
    const file_path = ctx.getString("file") orelse "";
    if (file_path.len == 0) {
        ctx.printErr("Error: --file is required\n", .{});
        return error.CommandFailed;
    }

    const content = readFile(ctx, file_path) orelse return error.CommandFailed;
    defer ctx.allocator.free(content);

    // Phase 1: Parse
    var def = proc_parser.parseJobDefinition(ctx.allocator, content) catch |err| {
        ctx.printErr("FAIL  Parse error: {s}\n", .{processingParseErrorString(err)});
        return error.CommandFailed;
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
        return error.CommandFailed;
    } else if (warnings > 0) {
        ctx.print("PASSED with {d} warning(s)\n", .{warnings});
    } else {
        ctx.print("PASSED: processing definition is valid\n", .{});
    }
}

fn processingParseErrorString(err: proc_parser.ParseError) []const u8 {
    return switch (err) {
        error.MissingRequiredField => "missing required field (kind or name)",
        error.InvalidKind => "kind must be 'Processing'",
        error.MissingSource => "missing source definition",
        error.MissingSink => "missing sink definition",
        error.MissingSourceStream => "source is missing stream name",
        error.MissingSinkTarget => "sink is missing target name",
        error.InvalidParallelism => "parallelism must be a positive integer",
        error.InvalidPartitions => "invalid partitions value",
        error.InvalidFormat => "invalid YAML/JSON format",
        error.OutOfMemory => "out of memory",
    };
}

// =============================================================================
// Shared Helpers
// =============================================================================

fn readFile(ctx: *commander.Context, file_path: []const u8) ?[]u8 {
    const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
        ctx.printErr("Failed to open file '{s}': {}\n", .{ file_path, err });
        return null;
    };
    defer file.close();

    return file.readToEndAlloc(ctx.allocator, 4 * 1024 * 1024) catch |err| {
        ctx.printErr("Failed to read file: {}\n", .{err});
        return null;
    };
}
