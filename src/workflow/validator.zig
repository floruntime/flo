//! Workflow Definition Validator
//!
//! Validates workflow and plan definitions for correctness, including:
//! - Required fields present
//! - Transition targets are valid steps or terminals
//! - @actions/* and @plan/* references are properly formatted
//! - All steps are reachable from start
//! - No duplicate names
//!
//! # Usage
//!
//! ```zig
//! const validator = @import("workflow/validator.zig");
//!
//! // Validate workflow definition
//! var errors = try validator.validateWorkflow(allocator, &def);
//! defer errors.deinit(allocator);
//!
//! if (!errors.isEmpty()) {
//!     for (errors.items()) |err| {
//!         std.debug.print("Error: {s}\n", .{err.message});
//!     }
//! }
//! ```

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const definition = @import("definition.zig");
const plan_types = @import("plan_types.zig");
const cron = @import("cron.zig");

// Type imports
pub const WorkflowDefinition = definition.WorkflowDefinition;
pub const InlinePlan = definition.InlinePlan;
pub const Step = definition.Step;
pub const RunStep = definition.RunStep;
pub const WaitForSignalStep = definition.WaitForSignalStep;
pub const Transition = definition.Transition;
pub const Terminal = definition.Terminal;
pub const BuiltinTerminal = definition.BuiltinTerminal;
pub const NamedStep = definition.NamedStep;
pub const ExecutorConfig = plan_types.ExecutorConfig;

// =============================================================================
// Validation Error Types
// =============================================================================

pub const ErrorSeverity = enum {
    warning,
    @"error",

    pub fn toString(self: ErrorSeverity) []const u8 {
        return switch (self) {
            .warning => "warning",
            .@"error" => "error",
        };
    }
};

pub const ErrorCode = enum {
    // Structural errors (E1xx)
    missing_name,
    missing_version,
    missing_start,
    missing_executors,
    duplicate_step_name,
    duplicate_executor_name,
    duplicate_terminal_name,

    // Reference errors (E2xx)
    invalid_transition_target,
    invalid_action_reference,
    invalid_plan_reference,
    circular_reference,

    // Reachability errors (E3xx)
    unreachable_step,
    unreachable_terminal,
    no_terminal_path,

    // Configuration errors (E4xx)
    invalid_retry_config,
    invalid_breaker_config,
    invalid_rate_limit_config,
    invalid_cache_config,
    invalid_health_config,

    // Semantic errors (E5xx)
    conflicting_transitions,
    empty_transitions,
    signal_without_timeout,
    invalid_schedule_config,
    invalid_cron_expression,
    child_workflow_unverifiable,

    pub fn code(self: ErrorCode) []const u8 {
        return switch (self) {
            .missing_name => "E101",
            .missing_version => "E102",
            .missing_start => "E103",
            .missing_executors => "E104",
            .duplicate_step_name => "E105",
            .duplicate_executor_name => "E106",
            .duplicate_terminal_name => "E107",

            .invalid_transition_target => "E201",
            .invalid_action_reference => "E202",
            .invalid_plan_reference => "E203",
            .circular_reference => "E204",

            .unreachable_step => "E301",
            .unreachable_terminal => "E302",
            .no_terminal_path => "E303",

            .invalid_retry_config => "E401",
            .invalid_breaker_config => "E402",
            .invalid_rate_limit_config => "E403",
            .invalid_cache_config => "E404",
            .invalid_health_config => "E405",

            .conflicting_transitions => "E501",
            .empty_transitions => "E502",
            .signal_without_timeout => "E503",
            .invalid_schedule_config => "E504",
            .invalid_cron_expression => "E506",
            .child_workflow_unverifiable => "E507",
        };
    }
};

pub const ValidationError = struct {
    code: ErrorCode,
    severity: ErrorSeverity,
    message: []const u8,
    location: ?[]const u8 = null,

    pub fn deinit(self: *ValidationError, allocator: Allocator) void {
        allocator.free(self.message);
        if (self.location) |loc| allocator.free(loc);
    }

    pub fn clone(self: ValidationError, allocator: Allocator) Allocator.Error!ValidationError {
        return ValidationError{
            .code = self.code,
            .severity = self.severity,
            .message = try allocator.dupe(u8, self.message),
            .location = if (self.location) |loc| try allocator.dupe(u8, loc) else null,
        };
    }
};

pub const ValidationResult = struct {
    errors: std.ArrayList(ValidationError),
    allocator: Allocator,

    pub fn init(allocator: Allocator) ValidationResult {
        return .{
            .errors = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ValidationResult) void {
        for (self.errors.items) |*err| {
            err.deinit(self.allocator);
        }
        self.errors.deinit(self.allocator);
    }

    pub fn isEmpty(self: ValidationResult) bool {
        return self.errors.items.len == 0;
    }

    pub fn hasErrors(self: ValidationResult) bool {
        for (self.errors.items) |err| {
            if (err.severity == .@"error") return true;
        }
        return false;
    }

    pub fn items(self: ValidationResult) []const ValidationError {
        return self.errors.items;
    }

    pub fn addError(self: *ValidationResult, code: ErrorCode, message: []const u8, location: ?[]const u8) !void {
        try self.errors.append(self.allocator, .{
            .code = code,
            .severity = .@"error",
            .message = try self.allocator.dupe(u8, message),
            .location = if (location) |loc| try self.allocator.dupe(u8, loc) else null,
        });
    }

    pub fn addWarning(self: *ValidationResult, code: ErrorCode, message: []const u8, location: ?[]const u8) !void {
        try self.errors.append(self.allocator, .{
            .code = code,
            .severity = .warning,
            .message = try self.allocator.dupe(u8, message),
            .location = if (location) |loc| try self.allocator.dupe(u8, loc) else null,
        });
    }
};

// =============================================================================
// Workflow Validator
// =============================================================================

pub fn validateWorkflow(allocator: Allocator, def: *const WorkflowDefinition) !ValidationResult {
    var result = ValidationResult.init(allocator);
    errdefer result.deinit();

    // 1. Required fields
    try validateRequiredFields(&result, def);

    // 2. Check for duplicate step names
    try validateNoDuplicateSteps(&result, def);

    // 3. Check for duplicate terminal names
    try validateNoDuplicateTerminals(&result, def);

    // 4. Validate all transition targets
    try validateTransitionTargets(&result, def);

    // 5. Validate action/plan references
    try validateReferences(&result, def);

    // 6. Reachability analysis
    try validateReachability(&result, def);

    // 7. Validate inline plans
    try validateInlinePlans(&result, def);

    // 8. Validate @plan/ references resolve to defined inline plans
    try validatePlanReferences(&result, def);

    // 9. Validate custom terminal status mappings
    try validateTerminalStatuses(&result, def);

    // 10. Check that at least one path reaches a terminal
    try validateTerminalPathExists(&result, def);

    // 11. Validate schedule config (cron/interval mutual exclusivity)
    try validateSchedule(&result, def);

    // 12. Note @workflow/ references that can only be verified at runtime
    try validateChildWorkflowReferences(&result, def);

    return result;
}

fn validateRequiredFields(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    if (def.name.len == 0) {
        try result.addError(.missing_name, "Workflow must have a name", null);
    }
    if (def.version.len == 0) {
        try result.addError(.missing_version, "Workflow must have a version", null);
    }
}

fn validateNoDuplicateSteps(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    var seen = std.StringHashMap(void).init(result.allocator);
    defer seen.deinit();

    for (def.steps) |step| {
        if (seen.contains(step.name)) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Duplicate step name: '{s}'", .{step.name}) catch "Duplicate step name";
            try result.addError(.duplicate_step_name, msg, step.name);
        } else {
            try seen.put(step.name, {});
        }
    }
}

fn validateNoDuplicateTerminals(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    var seen = std.StringHashMap(void).init(result.allocator);
    defer seen.deinit();

    // Add builtin terminals
    try seen.put("flo.Completed", {});
    try seen.put("flo.Failed", {});
    try seen.put("flo.Cancelled", {});
    try seen.put("flo.TimedOut", {});

    for (def.terminals) |terminal| {
        if (seen.contains(terminal.name)) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Duplicate terminal name: '{s}'", .{terminal.name}) catch "Duplicate terminal name";
            try result.addError(.duplicate_terminal_name, msg, terminal.name);
        } else {
            try seen.put(terminal.name, {});
        }
    }
}

fn validateTransitionTargets(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    // Collect all valid targets
    var valid_targets = std.StringHashMap(void).init(result.allocator);
    defer valid_targets.deinit();

    // Add step names
    for (def.steps) |step| {
        try valid_targets.put(step.name, {});
    }

    // Add terminal names
    for (def.terminals) |terminal| {
        try valid_targets.put(terminal.name, {});
    }

    // Add builtin terminals
    try valid_targets.put("flo.Completed", {});
    try valid_targets.put("flo.Failed", {});
    try valid_targets.put("flo.Cancelled", {});
    try valid_targets.put("flo.TimedOut", {});

    // Validate start step transitions
    try validateStepTransitions(result, def.start, "start", &valid_targets);

    // Validate all step transitions
    for (def.steps) |step| {
        try validateStepTransitions(result, step.step, step.name, &valid_targets);
    }
}

fn validateStepTransitions(result: *ValidationResult, step: Step, step_name: []const u8, valid_targets: *std.StringHashMap(void)) !void {
    const transitions = switch (step) {
        .run => |r| r.transitions,
        .wait_for_signal => |w| w.transitions,
    };

    if (transitions.len == 0) {
        var buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "Step '{s}' has no transitions", .{step_name}) catch "Step has no transitions";
        try result.addWarning(.empty_transitions, msg, step_name);
    }

    for (transitions) |trans| {
        if (!valid_targets.contains(trans.target)) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Invalid transition target '{s}' from step '{s}'", .{ trans.target, step_name }) catch "Invalid transition target";
            try result.addError(.invalid_transition_target, msg, step_name);
        }
    }

    // For wait_for_signal, check timeout target if present
    if (step == .wait_for_signal) {
        const wait = step.wait_for_signal;
        if (wait.on_timeout) |timeout_target| {
            if (!valid_targets.contains(timeout_target)) {
                var buf: [256]u8 = undefined;
                const msg = std.fmt.bufPrint(&buf, "Invalid timeout target '{s}' from step '{s}'", .{ timeout_target, step_name }) catch "Invalid timeout target";
                try result.addError(.invalid_transition_target, msg, step_name);
            }
        }
    }
}

fn validateReferences(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    // Validate start step reference
    try validateStepReference(result, def.start, "start");

    // Validate all step references
    for (def.steps) |step| {
        try validateStepReference(result, step.step, step.name);
    }
}

fn validateStepReference(result: *ValidationResult, step: Step, step_name: []const u8) !void {
    switch (step) {
        .run => |r| {
            if (!isValidActionOrPlanReference(r.target)) {
                var buf: [256]u8 = undefined;
                const msg = std.fmt.bufPrint(&buf, "Invalid action/plan reference '{s}' in step '{s}'", .{ r.target, step_name }) catch "Invalid reference";
                try result.addError(.invalid_action_reference, msg, step_name);
            }
        },
        .wait_for_signal => {}, // No references to validate
    }
}

fn isValidActionOrPlanReference(target: []const u8) bool {
    // Valid formats:
    // @actions/<name>
    // @plan/<name>
    // @workflow/<name> or @workflow/<name>:<version>
    if (mem.startsWith(u8, target, "@actions/")) {
        return target.len > "@actions/".len;
    }
    if (mem.startsWith(u8, target, "@plan/")) {
        return target.len > "@plan/".len;
    }
    if (mem.startsWith(u8, target, "@workflow/")) {
        return target.len > "@workflow/".len;
    }
    return false;
}

fn validateReachability(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    // BFS/DFS from start to find all reachable steps
    var visited = std.StringHashMap(void).init(result.allocator);
    defer visited.deinit();

    var queue: std.ArrayList([]const u8) = .empty;
    defer queue.deinit(result.allocator);

    // Start with the start step's transitions
    try visitStepTransitions(result.allocator, &queue, def.start);

    while (queue.items.len > 0) {
        const target = queue.orderedRemove(0);

        // Skip if already visited or is a terminal
        if (visited.contains(target)) continue;
        if (isTerminal(target, def)) continue;

        try visited.put(target, {});

        // Find the step and add its transitions
        for (def.steps) |step| {
            if (mem.eql(u8, step.name, target)) {
                try visitStepTransitions(result.allocator, &queue, step.step);
                break;
            }
        }
    }

    // Check if any steps are unreachable
    for (def.steps) |step| {
        if (!visited.contains(step.name)) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Step '{s}' is unreachable from start", .{step.name}) catch "Unreachable step";
            try result.addWarning(.unreachable_step, msg, step.name);
        }
    }
}

fn visitStepTransitions(allocator: Allocator, queue: *std.ArrayList([]const u8), step: Step) !void {
    const transitions = switch (step) {
        .run => |r| r.transitions,
        .wait_for_signal => |w| w.transitions,
    };

    for (transitions) |trans| {
        try queue.append(allocator, trans.target);
    }

    // Also add timeout target for wait_for_signal
    if (step == .wait_for_signal) {
        if (step.wait_for_signal.on_timeout) |timeout_target| {
            try queue.append(allocator, timeout_target);
        }
    }
}

fn isTerminal(name: []const u8, def: *const WorkflowDefinition) bool {
    // Check builtin terminals
    if (BuiltinTerminal.isBuiltin(name)) return true;

    // Check custom terminals
    for (def.terminals) |terminal| {
        if (mem.eql(u8, terminal.name, name)) return true;
    }

    return false;
}

// =============================================================================
// Inline Plan Validation (called from validateWorkflow)
// =============================================================================

fn validateInlinePlans(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    for (def.plans) |plan| {
        try validateInlinePlan(result, &plan);
    }
}

// =============================================================================
// @plan/ Reference Resolution
// =============================================================================

fn validatePlanReferences(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    // Build set of defined inline plan names
    var plan_names = std.StringHashMap(void).init(result.allocator);
    defer plan_names.deinit();
    for (def.plans) |p| {
        try plan_names.put(p.name, {});
    }

    // Check start step
    try validateStepPlanRef(result, def.start, "start", &plan_names);

    // Check all named steps
    for (def.steps) |step| {
        try validateStepPlanRef(result, step.step, step.name, &plan_names);
    }
}

fn validateStepPlanRef(result: *ValidationResult, step: Step, step_name: []const u8, plan_names: *std.StringHashMap(void)) !void {
    switch (step) {
        .run => |r| {
            if (mem.startsWith(u8, r.target, "@plan/")) {
                const plan_key = r.target["@plan/".len..];
                if (!plan_names.contains(plan_key)) {
                    var buf: [256]u8 = undefined;
                    const msg = std.fmt.bufPrint(&buf, "Step '{s}' references undefined plan '{s}'", .{ step_name, r.target }) catch "Undefined plan reference";
                    try result.addError(.invalid_plan_reference, msg, step_name);
                }
            }
        },
        .wait_for_signal => {},
    }
}

// =============================================================================
// Custom Terminal Status Validation
// =============================================================================

fn validateTerminalStatuses(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    for (def.terminals) |t| {
        if (!t.status.isTerminal()) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Custom terminal '{s}' maps to non-terminal status '{s}'", .{ t.name, t.status.toString() }) catch "Invalid terminal status";
            try result.addError(.conflicting_transitions, msg, t.name);
        }
    }
}

// =============================================================================
// Terminal Path Existence
// =============================================================================

fn validateTerminalPathExists(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    // Check that at least one transition anywhere targets a terminal
    const start_transitions = switch (def.start) {
        .run => |r| r.transitions,
        .wait_for_signal => |w| w.transitions,
    };

    for (start_transitions) |t| {
        if (isTerminal(t.target, def)) return; // found one
    }

    for (def.steps) |step| {
        const transitions = switch (step.step) {
            .run => |r| r.transitions,
            .wait_for_signal => |w| w.transitions,
        };
        for (transitions) |t| {
            if (isTerminal(t.target, def)) return; // found one
        }
        // Also check timeout target
        if (step.step == .wait_for_signal) {
            if (step.step.wait_for_signal.on_timeout) |timeout_target| {
                if (isTerminal(timeout_target, def)) return;
            }
        }
    }

    try result.addWarning(.no_terminal_path, "No transition in the workflow targets a terminal state", null);
}

// =============================================================================
// Schedule Validation
// =============================================================================

fn validateSchedule(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    const sched = def.schedule orelse return;
    if (sched.cron_expr == null and sched.interval_ms == null) {
        try result.addError(.invalid_schedule_config, "Schedule defined but missing both 'cron' and 'interval'", null);
    }
    if (sched.cron_expr != null and sched.interval_ms != null) {
        try result.addError(.invalid_schedule_config, "Schedule has both 'cron' and 'interval' (mutually exclusive)", null);
    }
    if (sched.cron_expr) |expr| {
        try validateCronExpr(result, expr);
    }
    if (sched.interval_ms) |ms| {
        if (ms <= 0) {
            try result.addError(.invalid_schedule_config, "Schedule interval must be positive", null);
        }
    }
}

/// Validate a 5-field cron expression by parsing each field.
fn validateCronExpr(result: *ValidationResult, expr: []const u8) !void {
    // Split into whitespace-separated fields
    var fields: [5][]const u8 = undefined;
    var field_count: usize = 0;
    var start: usize = 0;

    for (expr, 0..) |c, i| {
        if (c == ' ' or c == '\t') {
            if (i > start and field_count < 5) {
                fields[field_count] = expr[start..i];
                field_count += 1;
            }
            start = i + 1;
        }
    }
    if (start < expr.len and field_count < 5) {
        fields[field_count] = expr[start..];
        field_count += 1;
    }

    if (field_count != 5) {
        try result.addError(.invalid_cron_expression, "Cron expression must have exactly 5 fields (minute hour day month weekday)", "schedule.cron");
        return;
    }

    const field_names = [_][]const u8{ "minute", "hour", "day-of-month", "month", "day-of-week" };
    const field_ranges = [_][2]u8{ .{ 0, 59 }, .{ 0, 23 }, .{ 1, 31 }, .{ 1, 12 }, .{ 0, 7 } };

    for (0..5) |i| {
        if (cron.parseCronField(fields[i], field_ranges[i][0], field_ranges[i][1]) == null) {
            var buf: [128]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Invalid cron {s} field: '{s}'", .{ field_names[i], fields[i] }) catch "Invalid cron field";
            try result.addError(.invalid_cron_expression, msg, "schedule.cron");
        }
    }
}

// =============================================================================
// Child Workflow Reference Hints
// =============================================================================

fn validateChildWorkflowReferences(result: *ValidationResult, def: *const WorkflowDefinition) !void {
    try checkStepChildWorkflow(result, def.start, "start");
    for (def.steps) |step| {
        try checkStepChildWorkflow(result, step.step, step.name);
    }
}

fn checkStepChildWorkflow(result: *ValidationResult, step: Step, step_name: []const u8) !void {
    switch (step) {
        .run => |r| {
            if (mem.startsWith(u8, r.target, "@workflow/")) {
                var buf: [256]u8 = undefined;
                const msg = std.fmt.bufPrint(&buf, "Step '{s}' references child workflow '{s}' (existence verified at runtime by server)", .{ step_name, r.target }) catch "Child workflow reference";
                try result.addWarning(.child_workflow_unverifiable, msg, step_name);
            }
        },
        .wait_for_signal => {},
    }
}

/// Validate an inline plan within a workflow
pub fn validateInlinePlan(result: *ValidationResult, plan: *const InlinePlan) !void {
    // Must have at least one executor
    if (plan.executors.len == 0) {
        var buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "Inline plan '{s}' must have at least one executor", .{plan.name}) catch "Plan missing executors";
        try result.addError(.missing_executors, msg, plan.name);
    }

    // Check for duplicate executor names
    var seen = std.StringHashMap(void).init(result.allocator);
    defer seen.deinit();

    for (plan.executors) |exec| {
        if (seen.contains(exec.name)) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Duplicate executor name '{s}' in plan '{s}'", .{ exec.name, plan.name }) catch "Duplicate executor name";
            try result.addError(.duplicate_executor_name, msg, exec.name);
        } else {
            try seen.put(exec.name, {});
        }
    }

    // Validate executor configurations
    for (plan.executors) |exec| {
        try validateExecutorConfig(result, exec, plan.name);
    }

    // Validate action references in executors (executors can only reference @actions/*)
    for (plan.executors) |exec| {
        if (!isValidActionReference(exec.action_name)) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Invalid action reference '{s}' in executor '{s}' (plan '{s}')", .{ exec.action_name, exec.name, plan.name }) catch "Invalid reference";
            try result.addError(.invalid_action_reference, msg, exec.name);
        }
    }

    // Validate health config if present
    if (plan.health_config) |health| {
        if (health.decay <= 0.0 or health.decay > 1.0) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Plan '{s}' health decay must be in range (0, 1]", .{plan.name}) catch "Invalid health config";
            try result.addError(.invalid_health_config, msg, plan.name);
        }
        if (health.min_samples == 0) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Plan '{s}' health min_samples of 0 may cause unstable routing", .{plan.name}) catch "Invalid health config";
            try result.addWarning(.invalid_health_config, msg, plan.name);
        }
        if (health.window_ms <= 0) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Plan '{s}' health window_ms must be positive", .{plan.name}) catch "Invalid health config";
            try result.addError(.invalid_health_config, msg, plan.name);
        }
    }

    // Validate cache config if present
    if (plan.cache_config) |cache| {
        if (cache.ttl_ms <= 0) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Plan '{s}' cache ttl_ms must be positive", .{plan.name}) catch "Invalid cache config";
            try result.addError(.invalid_cache_config, msg, plan.name);
        }
        if (cache.key_template.len == 0) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Plan '{s}' cache key_template cannot be empty", .{plan.name}) catch "Invalid cache config";
            try result.addError(.invalid_cache_config, msg, plan.name);
        }
    }
}

/// Validate a single executor configuration
fn validateExecutorConfig(result: *ValidationResult, exec: ExecutorConfig, plan_name: []const u8) !void {
    // Validate retry config
    if (exec.retry) |retry| {
        if (retry.max_attempts == 0) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Executor '{s}' (plan '{s}') has invalid retry max_attempts (must be > 0)", .{ exec.name, plan_name }) catch "Invalid retry config";
            try result.addError(.invalid_retry_config, msg, exec.name);
        }
        if (retry.initial_delay_ms > retry.max_delay_ms) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Executor '{s}' (plan '{s}') has initial_delay_ms > max_delay_ms", .{ exec.name, plan_name }) catch "Invalid retry config";
            try result.addError(.invalid_retry_config, msg, exec.name);
        }
    }

    // Validate circuit breaker config
    if (exec.breaker) |breaker| {
        if (breaker.failure_threshold == 0) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Executor '{s}' (plan '{s}') has invalid breaker failure_threshold (must be > 0)", .{ exec.name, plan_name }) catch "Invalid breaker config";
            try result.addError(.invalid_breaker_config, msg, exec.name);
        }
        if (breaker.half_open_max_calls == 0) {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Executor '{s}' (plan '{s}') has invalid breaker half_open_max_calls (must be > 0)", .{ exec.name, plan_name }) catch "Invalid breaker config";
            try result.addError(.invalid_breaker_config, msg, exec.name);
        }
    }

    // Validate rate limit config
    if (exec.rate_limit) |rate_limit| {
        if (rate_limit.max_per_second == null and
            rate_limit.max_per_minute == null and
            rate_limit.max_per_hour == null)
        {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "Executor '{s}' (plan '{s}') has rate_limit with no limits set", .{ exec.name, plan_name }) catch "Invalid rate limit config";
            try result.addWarning(.invalid_rate_limit_config, msg, exec.name);
        }
    }
}

fn isValidActionReference(target: []const u8) bool {
    // Plan executors can only reference actions, not other plans
    if (mem.startsWith(u8, target, "@actions/")) {
        return target.len > "@actions/".len;
    }
    return false;
}

// =============================================================================
// Utility Functions
// =============================================================================

/// Format a validation result as a human-readable string
pub fn formatResult(allocator: Allocator, result: *const ValidationResult) ![]u8 {
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(allocator);

    const writer = output.writer(allocator);

    var error_count: usize = 0;
    var warning_count: usize = 0;

    for (result.errors.items) |err| {
        if (err.severity == .@"error") {
            error_count += 1;
        } else {
            warning_count += 1;
        }

        try writer.print("[{s}] {s}: {s}", .{ err.code.code(), err.severity.toString(), err.message });
        if (err.location) |loc| {
            try writer.print(" at '{s}'", .{loc});
        }
        try writer.writeByte('\n');
    }

    try writer.print("\nTotal: {d} error(s), {d} warning(s)\n", .{ error_count, warning_count });

    return output.toOwnedSlice(allocator);
}

// =============================================================================
// Tests
// =============================================================================

test "validateWorkflow: valid workflow" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Create a valid workflow
    var def = WorkflowDefinition{
        .name = "test-workflow",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/validate",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "process" },
                    .{ .outcome = "failure", .target = "flo.Failed" },
                },
            },
        },
        .steps = &.{
            .{
                .name = "process",
                .step = .{
                    .run = .{
                        .target = "@actions/processing",
                        .input_mapping = null,
                        .retry = null,
                        .poll = null,
                        .transitions = &.{
                            .{ .outcome = "success", .target = "flo.Completed" },
                            .{ .outcome = "failure", .target = "flo.Failed" },
                        },
                    },
                },
            },
        },
        .terminals = &.{},
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    // Should have no errors
    try testing.expect(!result.hasErrors());
}

test "validateWorkflow: missing name" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/test",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    try testing.expect(result.hasErrors());
    try testing.expectEqual(ErrorCode.missing_name, result.errors.items[0].code);
}

test "validateWorkflow: invalid transition target" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/test",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "nonexistent_step" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    try testing.expect(result.hasErrors());

    var found_invalid_target = false;
    for (result.errors.items) |err| {
        if (err.code == .invalid_transition_target) {
            found_invalid_target = true;
            break;
        }
    }
    try testing.expect(found_invalid_target);
}

test "validateWorkflow: invalid action reference" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "invalid-reference", // Missing @actions/ or @plan/ prefix
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    try testing.expect(result.hasErrors());

    var found_invalid_ref = false;
    for (result.errors.items) |err| {
        if (err.code == .invalid_action_reference) {
            found_invalid_ref = true;
            break;
        }
    }
    try testing.expect(found_invalid_ref);
}

test "validateWorkflow: unreachable step warning" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/test",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                },
            },
        },
        .steps = &.{
            .{
                .name = "orphan_step",
                .step = .{
                    .run = .{
                        .target = "@actions/orphan",
                        .input_mapping = null,
                        .retry = null,
                        .poll = null,
                        .transitions = &.{
                            .{ .outcome = "success", .target = "flo.Completed" },
                        },
                    },
                },
            },
        },
        .terminals = &.{},
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    // Should have a warning but no error
    try testing.expect(!result.isEmpty());

    var found_unreachable = false;
    for (result.errors.items) |err| {
        if (err.code == .unreachable_step) {
            found_unreachable = true;
            try testing.expectEqual(ErrorSeverity.warning, err.severity);
            break;
        }
    }
    try testing.expect(found_unreachable);
}

test "isValidActionOrPlanReference" {
    const testing = std.testing;

    try testing.expect(isValidActionOrPlanReference("@actions/validate"));
    try testing.expect(isValidActionOrPlanReference("@plan/payment-processing"));
    try testing.expect(!isValidActionOrPlanReference("@actions/")); // Empty name
    try testing.expect(!isValidActionOrPlanReference("@plan/")); // Empty name
    try testing.expect(!isValidActionOrPlanReference("validate")); // No prefix
    try testing.expect(!isValidActionOrPlanReference("")); // Empty
}

test "validateWorkflow: undefined plan reference" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{}, // No plans defined
        .start = .{
            .run = .{
                .target = "@plan/nonexistent",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                    .{ .outcome = "failure", .target = "flo.Failed" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    try testing.expect(result.hasErrors());

    var found = false;
    for (result.errors.items) |err| {
        if (err.code == .invalid_plan_reference) {
            found = true;
            break;
        }
    }
    try testing.expect(found);
}

test "validateWorkflow: custom terminal with valid status" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/validate",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                    .{ .outcome = "failure", .target = "FraudDetected" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{
            .{ .name = "FraudDetected", .status = .failed },
        },
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    try testing.expect(!result.hasErrors());
}

test "validateWorkflow: no terminal path warning" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/validate",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "step_b" },
                },
            },
        },
        .steps = &.{
            .{
                .name = "step_b",
                .step = .{
                    .run = .{
                        .target = "@actions/process",
                        .input_mapping = null,
                        .retry = null,
                        .poll = null,
                        .transitions = &.{
                            .{ .outcome = "success", .target = "step_b" }, // loops forever
                        },
                    },
                },
            },
        },
        .terminals = &.{},
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    var found = false;
    for (result.errors.items) |err| {
        if (err.code == .no_terminal_path) {
            found = true;
            try testing.expectEqual(ErrorSeverity.warning, err.severity);
            break;
        }
    }
    try testing.expect(found);
}

test "validateWorkflow: schedule missing cron and interval" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/test",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
        .schedule = .{}, // empty — no cron, no interval
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    var found_sched = false;
    for (result.errors.items) |err| {
        if (err.code == .invalid_schedule_config) {
            found_sched = true;
            break;
        }
    }
    try testing.expect(found_sched);
}

test "validateWorkflow: schedule with both cron and interval" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/test",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
        .schedule = .{ .cron_expr = "*/5 * * * *", .interval_ms = 30000 },
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    var found_both = false;
    for (result.errors.items) |err| {
        if (err.code == .invalid_schedule_config) {
            found_both = true;
            break;
        }
    }
    try testing.expect(found_both);
}

test "validateWorkflow: child workflow reference warning" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@workflow/child-wf",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                    .{ .outcome = "failure", .target = "flo.Failed" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    // Should be a warning, not an error
    try testing.expect(!result.hasErrors());

    var found_cwf = false;
    for (result.errors.items) |err| {
        if (err.code == .child_workflow_unverifiable) {
            found_cwf = true;
            try testing.expectEqual(ErrorSeverity.warning, err.severity);
            break;
        }
    }
    try testing.expect(found_cwf);
}

test "validateWorkflow: valid cron expression passes" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/test",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
        .schedule = .{ .cron_expr = "0 9 * * 1-5" },
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    for (result.errors.items) |err| {
        if (err.code == .invalid_cron_expression) {
            try testing.expect(false); // should not have cron errors
        }
    }
}

test "validateWorkflow: invalid cron expression wrong field count" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/test",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
        .schedule = .{ .cron_expr = "*/5 * *" }, // only 3 fields
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    var found = false;
    for (result.errors.items) |err| {
        if (err.code == .invalid_cron_expression) {
            found = true;
            break;
        }
    }
    try testing.expect(found);
}

test "validateWorkflow: invalid cron expression bad field value" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/test",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
        .schedule = .{ .cron_expr = "99 * * * *" }, // 99 is out of range for minute (0-59)
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    var found = false;
    for (result.errors.items) |err| {
        if (err.code == .invalid_cron_expression) {
            found = true;
            break;
        }
    }
    try testing.expect(found);
}

test "validateWorkflow: negative interval rejected" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var def = WorkflowDefinition{
        .name = "test",
        .description = "",
        .version = "1.0.0",
        .idempotency = .none,
        .search_attributes = &.{},
        .plans = &.{},
        .start = .{
            .run = .{
                .target = "@actions/test",
                .input_mapping = null,
                .retry = null,
                .poll = null,
                .transitions = &.{
                    .{ .outcome = "success", .target = "flo.Completed" },
                },
            },
        },
        .steps = &.{},
        .terminals = &.{},
        .schedule = .{ .interval_ms = -1000 },
    };

    var result = try validateWorkflow(allocator, &def);
    defer result.deinit();

    var found = false;
    for (result.errors.items) |err| {
        if (err.code == .invalid_schedule_config) {
            found = true;
            break;
        }
    }
    try testing.expect(found);
}
