//! Dashboard API — Workflow Endpoints
//!
//! - GET    /workflow/definitions          — List workflow definitions
//! - POST   /workflow/definitions          — Create definition
//! - GET    /workflow/definitions/:name    — Get definition
//! - PUT    /workflow/definitions/:name/enable  — Enable workflow
//! - PUT    /workflow/definitions/:name/disable — Disable workflow
//! - GET    /workflow/runs                 — List runs
//! - POST   /workflow/runs                 — Start run
//! - GET    /workflow/runs/:id             — Run status
//! - GET    /workflow/runs/:id/history     — Run step history
//! - DELETE /workflow/runs/:id             — Cancel run
//! - POST   /workflow/runs/:id/signal      — Send signal

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Method = @import("../../../util/http/mod.zig").Method;
const Shard = @import("../../shard.zig").Shard;
const WorkflowHandler = @import("../../../workflow/handler.zig").WorkflowHandler;
const wf_parser = @import("../../../workflow/parser.zig");
const wf_definition = @import("../../../workflow/definition.zig");

// ── Helpers ──

fn getShard(ctx: *DashboardContext, idx: usize) ?*Shard {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    return @ptrCast(@alignCast(ptrs[idx]));
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

/// Router for /workflow/* requests
pub fn handleWorkflowRequest(allocator: Allocator, method: Method, path: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    // /workflow/definitions
    if (std.mem.eql(u8, path, "/definitions")) {
        return switch (method) {
            .GET => listDefinitions(allocator, query_string, ctx),
            .POST => createDefinition(allocator, body, ctx),
            else => h.jsonError(allocator, "Method not allowed"),
        };
    }

    // /workflow/runs (exact match)
    if (std.mem.eql(u8, path, "/runs")) {
        return switch (method) {
            .GET => listRuns(allocator, query_string, ctx),
            .POST => startRun(allocator, body, ctx),
            else => h.jsonError(allocator, "Method not allowed"),
        };
    }

    // /workflow/definitions/:name[/enable|/disable]
    if (std.mem.startsWith(u8, path, "/definitions/")) {
        const rest = path["/definitions/".len..];
        if (std.mem.endsWith(u8, rest, "/enable")) {
            const name = rest[0 .. rest.len - "/enable".len];
            return enableWorkflow(allocator, name, ctx);
        }
        if (std.mem.endsWith(u8, rest, "/disable")) {
            const name = rest[0 .. rest.len - "/disable".len];
            return disableWorkflow(allocator, name, ctx);
        }
        // /workflow/definitions/:name
        return getDefinition(allocator, rest, ctx);
    }

    // /workflow/runs/:id[/history|/signal]
    if (std.mem.startsWith(u8, path, "/runs/")) {
        const rest = path["/runs/".len..];
        if (std.mem.endsWith(u8, rest, "/history")) {
            const run_id = rest[0 .. rest.len - "/history".len];
            return getRunHistory(allocator, run_id, ctx);
        }
        if (std.mem.endsWith(u8, rest, "/signal")) {
            const run_id = rest[0 .. rest.len - "/signal".len];
            return signalRun(allocator, run_id, body, ctx);
        }
        // DELETE /workflow/runs/:id or GET /workflow/runs/:id
        return switch (method) {
            .DELETE => cancelRun(allocator, rest, ctx),
            .GET => getRunStatus(allocator, rest, ctx),
            else => h.jsonError(allocator, "Method not allowed"),
        };
    }

    return h.jsonError(allocator, "Not found");
}

// ---------------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------------

fn listDefinitions(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_filter = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Collect definitions from all shards (de-duplicate by name)
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            const wh = shard.workflow_handler;
            var it = wh.definitions.iterator();
            while (it.next()) |entry| {
                const rec = entry.value_ptr;
                const map_key = entry.key_ptr.*;
                // Filter by namespace (defaults to "default", map key is "namespace:name")
                if (std.mem.indexOfScalar(u8, map_key, ':')) |colon| {
                    if (!std.mem.eql(u8, map_key[0..colon], ns_filter)) continue;
                }
                const gop = try seen.getOrPut(rec.name_owned);
                if (!gop.found_existing) {
                    try arr.next();
                    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try obj.begin();
                    try obj.stringField("name", rec.name_owned);
                    try obj.stringField("version", rec.version_owned);
                    try obj.intField("created_at", rec.created_at_ms);
                    // Check if disabled
                    const is_disabled = wh.disabled.contains(entry.key_ptr.*);
                    try obj.boolField("enabled", !is_disabled);

                    // Parse YAML to extract step metadata
                    if (wf_parser.parseWorkflow(allocator, rec.yaml_owned)) |parsed| {
                        var def = parsed;
                        defer def.deinit(allocator);
                        try def.writeJsonMeta(&obj);
                    } else |_| {
                        try wf_definition.WorkflowDefinition.writeJsonMetaEmpty(&obj);
                    }
                    try obj.end();
                }
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn createDefinition(allocator: Allocator, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    if (body.len == 0) return try h.jsonError(allocator, "Empty definition body");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Write operations require Raft proposal — not safe from dashboard thread
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("status", "not_wired");
    try obj.intField("body_size", @as(i64, @intCast(body.len)));
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn getDefinition(allocator: Allocator, name: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Search shards for this definition
    var found_rec: ?*const WorkflowHandler.DefinitionRecord = null;
    var found_handler: ?*const WorkflowHandler = null;
    var found_key: ?[]const u8 = null;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            const wh = shard.workflow_handler;
            // Definitions are keyed by "namespace:name"; try the name directly first
            if (wh.definitions.getPtr(name)) |rec| {
                found_rec = rec;
                found_handler = wh;
                found_key = name;
                break;
            }
            // Also search by matching the record's name_owned field
            var it = wh.definitions.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.value_ptr.name_owned, name)) {
                    found_rec = entry.value_ptr;
                    found_handler = wh;
                    found_key = entry.key_ptr.*;
                    break;
                }
            }
            if (found_rec != null) break;
        }
    }

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);

    if (found_rec) |rec| {
        try obj.stringField("version", rec.version_owned);
        try obj.intField("created_at", rec.created_at_ms);
        const is_disabled = if (found_handler) |wh_ptr| (if (found_key) |k| wh_ptr.disabled.contains(k) else false) else false;
        try obj.boolField("enabled", !is_disabled);
        try obj.stringField("status", if (is_disabled) "disabled" else "active");
        try obj.stringField("definition_yaml", rec.yaml_owned);

        // Count runs for this workflow
        var run_count: u64 = 0;
        for (0..n) |i| {
            if (getShard(ctx, i)) |shard| {
                var rit = shard.workflow_handler.runs.iterator();
                while (rit.next()) |re| {
                    if (std.mem.eql(u8, re.value_ptr.workflow_name_owned, name)) {
                        run_count += 1;
                    }
                }
            }
        }
        try obj.intField("run_count", @as(i64, @intCast(run_count)));
    } else {
        try obj.stringField("status", "unknown");
        try obj.boolField("enabled", false);
        try obj.intField("run_count", 0);
    }

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn enableWorkflow(allocator: Allocator, name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);
    try obj.boolField("enabled", true);
    try obj.stringField("status", "not_wired");
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn disableWorkflow(allocator: Allocator, name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);
    try obj.boolField("enabled", false);
    try obj.stringField("status", "not_wired");
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

fn listRuns(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const workflow_filter = h.parseQueryParam([]const u8, query_string, "workflow");
    const status_filter = h.parseQueryParam([]const u8, query_string, "status");
    const ns_filter = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const limit = h.parseQueryParam(u64, query_string, "limit") orelse 100;
    const search_query = h.parseQueryParam([]const u8, query_string, "search");
    const cursor = h.parseQueryParam([]const u8, query_string, "cursor");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Lowercase the search query for case-insensitive matching
    var search_lower_buf: [256]u8 = undefined;
    const search_lower: ?[]const u8 = if (search_query) |sq| blk: {
        const len = @min(sq.len, search_lower_buf.len);
        for (0..len) |idx| {
            search_lower_buf[idx] = std.ascii.toLower(sq[idx]);
        }
        break :blk search_lower_buf[0..len];
    } else null;

    // Collect all matching runs first so we can sort by started_at descending
    const RunRef = struct {
        run: *WorkflowHandler.RunRecord,
        started_at: i64,
    };
    var all_runs: std.ArrayList(RunRef) = .empty;
    defer all_runs.deinit(allocator);

    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            var it = shard.workflow_handler.runs.iterator();
            while (it.next()) |entry| {
                const run = entry.value_ptr;
                const map_key = entry.key_ptr.*;
                // Filter by namespace (defaults to "default", map key is "namespace:run_id")
                if (std.mem.indexOfScalar(u8, map_key, ':')) |colon| {
                    if (!std.mem.eql(u8, map_key[0..colon], ns_filter)) continue;
                }
                // Filter by workflow name if specified
                if (workflow_filter) |wf| {
                    if (!std.mem.eql(u8, run.workflow_name_owned, wf)) continue;
                }
                // Filter by status if specified
                const status_str = run.status.toString();
                if (status_filter) |sf| {
                    if (!std.mem.eql(u8, status_str, sf)) continue;
                }
                // Free-text search: match against run_id, workflow name, current step, and input
                if (search_lower) |sq| {
                    const matched = containsLower(run.run_id_owned, sq) or
                        containsLower(run.workflow_name_owned, sq) or
                        (if (run.current_step_name_owned) |step| containsLower(step, sq) else false) or
                        containsLower(run.input_owned, sq) or
                        (if (run.search_tags_owned) |tags| containsLower(tags, sq) else false);
                    if (!matched) continue;
                }
                const started = run.started_at_ms orelse run.created_at_ms;
                try all_runs.append(allocator, .{ .run = run, .started_at = started });
            }
        }
    }

    // Sort by started_at descending
    std.mem.sortUnstable(RunRef, all_runs.items, {}, struct {
        fn lessThan(_: void, a: RunRef, b: RunRef) bool {
            return a.started_at > b.started_at; // descending
        }
    }.lessThan);

    // Apply cursor-based pagination: skip runs until we find the cursor run_id
    var start_idx: usize = 0;
    if (cursor) |c| {
        for (all_runs.items, 0..) |item, idx| {
            if (std.mem.eql(u8, item.run.run_id_owned, c)) {
                start_idx = idx + 1; // Start after the cursor
                break;
            }
        }
    }

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    const end_idx = @min(start_idx + limit, all_runs.items.len);
    for (all_runs.items[start_idx..end_idx]) |item| {
        const run = item.run;
                const run_status_str = run.status.toString();
                // Derive trigger source from first history event
                const triggered_by: []const u8 = if (run.history.items.len > 0) blk: {
                    const first = run.history.items[0].event_type_owned;
                    if (std.mem.eql(u8, first, "schedule_started")) break :blk "schedule";
                    if (std.mem.eql(u8, first, "trigger_started")) break :blk "stream";
                    break :blk "manual";
                } else "manual";
                try arr.next();
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("run_id", run.run_id_owned);
                try obj.stringField("workflow", run.workflow_name_owned);
                // Resolve actual version from definitions ("latest" → real version)
                const resolved_version = blk: {
                    if (!std.mem.eql(u8, run.workflow_version_owned, "latest")) break :blk run.workflow_version_owned;
                    // Search definitions for the real version
                    for (0..n) |si| {
                        if (getShard(ctx, si)) |s| {
                            var dit = s.workflow_handler.definitions.iterator();
                            while (dit.next()) |de| {
                                if (std.mem.eql(u8, de.value_ptr.name_owned, run.workflow_name_owned)) {
                                    break :blk de.value_ptr.version_owned;
                                }
                            }
                        }
                    }
                    break :blk run.workflow_version_owned;
                };
                try obj.stringField("version", resolved_version);
                try obj.stringField("status", run_status_str);
                try obj.stringField("triggered_by", triggered_by);
                if (run.current_step_name_owned) |step| {
                    try obj.stringField("current_step", step);
                } else {
                    try obj.nullField("current_step");
                }
                try obj.intField("started_at", item.started_at);
                if (run.completed_at_ms) |t| {
                    try obj.intField("completed_at", t);
                    try obj.intField("duration_ms", t - item.started_at);
                } else {
                    try obj.nullField("completed_at");
                    try obj.nullField("duration_ms");
                }
                if (run.wait_signal_type_owned) |wt| {
                    // Hide internal _action_done:* synthetic signals from the API
                    if (std.mem.startsWith(u8, wt, "_action_done:")) {
                        try obj.stringField("wait_type", "action");
                    } else {
                        try obj.stringField("wait_type", wt);
                    }
                } else {
                    try obj.nullField("wait_type");
                }
                try obj.nullField("parent_run_id");
                try obj.nullField("terminal_name");
                if (run.history.items.len > 0) {
                    // Check last event for error info
                    const last = run.history.items[run.history.items.len - 1];
                    if (std.mem.eql(u8, last.event_type_owned, "workflow_failed") or
                        std.mem.eql(u8, last.event_type_owned, "step_failed"))
                    {
                        try obj.stringField("error", last.detail_owned);
                    } else {
                        try obj.nullField("error");
                    }
                } else {
                    try obj.nullField("error");
                }
                try obj.intField("history_event_count", @as(i64, @intCast(run.history.items.len)));
                try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn startRun(allocator: Allocator, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    if (body.len == 0) return try h.jsonError(allocator, "Empty run request body");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("status", "not_wired");
    try obj.intField("body_size", @as(i64, @intCast(body.len)));
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn getRunStatus(allocator: Allocator, run_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Search for the run across shards
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            const run_ptr = shard.workflow_handler.runs.getPtr(run_id) orelse blk: {
                // Fallback: linear scan by run_id_owned
                var rit = shard.workflow_handler.runs.iterator();
                while (rit.next()) |entry| {
                    if (std.mem.eql(u8, entry.value_ptr.run_id_owned, run_id)) {
                        break :blk entry.value_ptr;
                    }
                }
                break :blk null;
            };
            if (run_ptr) |run| {
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("run_id", run.run_id_owned);
                try obj.stringField("workflow", run.workflow_name_owned);
                // Resolve actual version
                const resolved_version = blk: {
                    if (!std.mem.eql(u8, run.workflow_version_owned, "latest")) break :blk run.workflow_version_owned;
                    for (0..n) |si| {
                        if (getShard(ctx, si)) |s| {
                            var dit = s.workflow_handler.definitions.iterator();
                            while (dit.next()) |de| {
                                if (std.mem.eql(u8, de.value_ptr.name_owned, run.workflow_name_owned)) {
                                    break :blk de.value_ptr.version_owned;
                                }
                            }
                        }
                    }
                    break :blk run.workflow_version_owned;
                };
                try obj.stringField("version", resolved_version);
                try obj.stringField("status", run.status.toString());
                if (run.current_step_name_owned) |step| {
                    try obj.stringField("current_step", step);
                } else {
                    try obj.nullField("current_step");
                }
                if (run.started_at_ms) |t| {
                    try obj.intField("started_at_ms", t);
                }
                if (run.completed_at_ms) |t| {
                    try obj.intField("completed_at", t);
                    try obj.intField("updated_at_ms", t);
                    const start = run.started_at_ms orelse run.created_at_ms;
                    try obj.intField("duration_ms", t - start);
                }
                // Error message (from last failed event)
                if (run.history.items.len > 0) {
                    const last = run.history.items[run.history.items.len - 1];
                    if (std.mem.eql(u8, last.event_type_owned, "workflow_failed") or
                        std.mem.eql(u8, last.event_type_owned, "step_failed"))
                    {
                        try obj.stringField("error_message", last.detail_owned);
                    }
                }
                // Input (raw JSON string — write as-is)
                if (run.input_owned.len > 0) {
                    try obj.field("input");
                    try writer.writeAll(run.input_owned);
                }
                if (run.output_owned) |output| {
                    try obj.field("output");
                    try writer.writeAll(output);
                } else {
                    try obj.nullField("output");
                }
                // Step outputs → step_results object
                if (run.step_outputs) |outputs| {
                    if (outputs.entries.len > 0) {
                        var sobj = try obj.objectField("step_results");
                        try sobj.begin();
                        for (outputs.entries) |entry| {
                            var eobj = try sobj.objectField(entry.step_name);
                            try eobj.begin();
                            try eobj.stringField("outcome", entry.outcome);
                            if (entry.output.len > 0) {
                                try eobj.field("output");
                                try writer.writeAll(entry.output);
                            } else {
                                try eobj.nullField("output");
                            }
                            try eobj.end();
                        }
                        try sobj.end();
                    }
                }
                // Derive trigger source
                const triggered_by: []const u8 = if (run.history.items.len > 0) tblk: {
                    const first = run.history.items[0].event_type_owned;
                    if (std.mem.eql(u8, first, "schedule_started")) break :tblk "schedule";
                    if (std.mem.eql(u8, first, "trigger_started")) break :tblk "stream";
                    break :tblk "manual";
                } else "manual";
                try obj.stringField("triggered_by", triggered_by);

                // Extract search attributes from definition + input
                try emitSearchAttributes(allocator, &obj, writer, run, ctx);

                // Signals
                try obj.intField("pending_signals", @as(i64, @intCast(run.signals.items.len)));
                // History count
                try obj.intField("history_event_count", @as(i64, @intCast(run.history.items.len)));
                try obj.end();
                return try json_buf.toOwnedSlice(allocator);
            }
        }
    }

    // Not found
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("error", "Run not found");
    try obj.stringField("run_id", run_id);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn getRunHistory(allocator: Allocator, run_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Search for the run across shards
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            if (shard.workflow_handler.runs.getPtr(run_id)) |run| {
                // Found the run — emit its history events
                for (run.history.items) |event| {
                    try arr.next();
                    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try obj.begin();
                    try obj.stringField("event_type", event.event_type_owned);
                    try obj.stringField("step_name", event.detail_owned);
                    try obj.intField("timestamp", event.timestamp_ms);
                    try obj.end();
                }
                break; // found it, no need to search more shards
            }
            // Also search by record's run_id field
            var rit = shard.workflow_handler.runs.iterator();
            while (rit.next()) |entry| {
                if (std.mem.eql(u8, entry.value_ptr.run_id_owned, run_id)) {
                    for (entry.value_ptr.history.items) |event| {
                        try arr.next();
                        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                        try obj.begin();
                        try obj.stringField("event_type", event.event_type_owned);
                        try obj.stringField("step_name", event.detail_owned);
                        try obj.intField("timestamp", event.timestamp_ms);
                        try obj.end();
                    }
                    break;
                }
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn cancelRun(allocator: Allocator, run_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("run_id", run_id);
    try obj.stringField("status", "not_wired");
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn signalRun(allocator: Allocator, run_id: []const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("run_id", run_id);
    try obj.stringField("status", "not_wired");
    try obj.intField("signal_size", @as(i64, @intCast(body.len)));
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// Search Attribute Extraction
// =============================================================================

/// Look up definition for a run, extract search attributes from its input,
/// and emit a "search_attributes" JSON object into the response builder.
fn emitSearchAttributes(
    allocator: Allocator,
    obj: anytype,
    writer: anytype,
    run: *const WorkflowHandler.RunRecord,
    ctx: *DashboardContext,
) !void {
    // Use pre-computed search tags if available (built at run start/completion)
    if (run.search_tags_owned) |tags| {
        try obj.field("search_attributes");
        try writer.writeAll(tags);
        return;
    }

    // Fall back to re-parsing definition for old runs without pre-computed tags
    const n = shardCount(ctx);
    var yaml: ?[]const u8 = null;
    for (0..n) |si| {
        if (getShard(ctx, si)) |s| {
            var dit = s.workflow_handler.definitions.iterator();
            while (dit.next()) |de| {
                if (std.mem.eql(u8, de.value_ptr.name_owned, run.workflow_name_owned)) {
                    yaml = de.value_ptr.yaml_owned;
                    break;
                }
            }
            if (yaml != null) break;
        }
    }
    const def_yaml = yaml orelse return;

    // Parse definition to get search_attributes
    var def = wf_parser.parseWorkflow(allocator, def_yaml) catch return;
    defer def.deinit(allocator);

    if (def.search_attributes.len == 0) return;

    // Parse the run's input JSON (for $.input.* resolution)
    const parsed_input = std.json.parseFromSlice(std.json.Value, allocator, run.input_owned, .{}) catch return;
    defer parsed_input.deinit();

    const input_obj: ?std.json.ObjectMap = switch (parsed_input.value) {
        .object => |o| o,
        else => null,
    };

    // Emit "search_attributes": { ... }
    var sobj = try obj.objectField("search_attributes");
    try sobj.begin();

    for (def.search_attributes) |attr| {
        const extracted = resolveSearchAttrPath(attr.from, input_obj, run);

        switch (attr.attr_type) {
            .string => {
                if (extracted) |val| {
                    switch (val) {
                        .string => |s| try sobj.stringField(attr.name, s),
                        .integer => |i| {
                            var buf: [24]u8 = undefined;
                            const s = std.fmt.bufPrint(&buf, "{d}", .{i}) catch continue;
                            try sobj.stringField(attr.name, s);
                        },
                        else => try sobj.nullField(attr.name),
                    }
                } else {
                    try sobj.nullField(attr.name);
                }
            },
            .number => {
                if (extracted) |val| {
                    switch (val) {
                        .integer => |i| try sobj.intField(attr.name, i),
                        .float => |f| {
                            // Write number directly
                            try sobj.field(attr.name);
                            var buf: [32]u8 = undefined;
                            const s = std.fmt.bufPrint(&buf, "{d}", .{f}) catch continue;
                            try writer.writeAll(s);
                        },
                        .string => |s| {
                            // Try parse string as number
                            const i = std.fmt.parseInt(i64, s, 10) catch {
                                try sobj.nullField(attr.name);
                                continue;
                            };
                            try sobj.intField(attr.name, i);
                        },
                        else => try sobj.nullField(attr.name),
                    }
                } else {
                    try sobj.nullField(attr.name);
                }
            },
            .timestamp => {
                if (extracted) |val| {
                    switch (val) {
                        .integer => |i| try sobj.intField(attr.name, i),
                        else => try sobj.nullField(attr.name),
                    }
                } else {
                    try sobj.nullField(attr.name);
                }
            },
        }
    }

    try sobj.end();
}

/// Resolve a search attribute `from` path against input JSON, step outputs, and flo metadata.
/// Supports: $.input.*, $.steps.{name}.output.*, $.flo.run_id, $.flo.timestamp,
/// and bare input.* paths for backward compatibility.
fn resolveSearchAttrPath(
    from: []const u8,
    input_obj: ?std.json.ObjectMap,
    run: *const WorkflowHandler.RunRecord,
) ?std.json.Value {
    // $.input.* — extract from workflow input JSON
    if (std.mem.startsWith(u8, from, "$.input.")) {
        const field_path = from[8..]; // Skip "$.input."
        const obj = input_obj orelse return null;
        return navigateJsonPath(obj, field_path);
    }

    // $.input — whole input object
    if (std.mem.eql(u8, from, "$.input")) {
        const obj = input_obj orelse return null;
        return .{ .object = obj };
    }

    // $.steps.{name}.output.* — extract from step output JSON
    if (std.mem.startsWith(u8, from, "$.steps.")) {
        const rest = from[8..]; // Skip "$.steps."
        const step_outputs = run.step_outputs orelse return null;
        // Find step name (until next '.')
        const dot_pos = std.mem.indexOf(u8, rest, ".") orelse return null;
        const step_name = rest[0..dot_pos];
        const after_step = rest[dot_pos + 1 ..];
        const step = step_outputs.get(step_name) orelse return null;

        if (std.mem.eql(u8, after_step, "output")) {
            // Return whole output as a string value
            return .{ .string = step.output };
        }
        if (std.mem.startsWith(u8, after_step, "output.")) {
            const field_path = after_step[7..]; // Skip "output."
            // Parse step output as JSON and navigate
            const parsed = std.json.parseFromSlice(
                std.json.Value,
                @import("std").heap.page_allocator,
                step.output,
                .{},
            ) catch return null;
            // Note: we intentionally leak here because the returned Value references
            // parsed memory. The caller only reads .string/.integer/.float which are
            // either slices into the original step.output bytes or inline values.
            switch (parsed.value) {
                .object => |o| return navigateJsonPath(o, field_path),
                else => return null,
            }
        }
        if (std.mem.eql(u8, after_step, "outcome")) {
            return .{ .string = step.outcome };
        }
        return null;
    }

    // $.flo.* — run metadata
    if (std.mem.startsWith(u8, from, "$.flo.")) {
        const field = from[6..]; // Skip "$.flo."
        if (std.mem.eql(u8, field, "run_id")) {
            return .{ .string = run.run_id_owned };
        }
        if (std.mem.eql(u8, field, "timestamp")) {
            return .{ .integer = std.time.milliTimestamp() };
        }
        return null;
    }

    return null;
}

/// Navigate a dot-separated path into a JSON object.
/// e.g. "customerId" → obj["customerId"], "shipping.region" → obj["shipping"]["region"]
fn navigateJsonPath(obj: std.json.ObjectMap, path: []const u8) ?std.json.Value {
    var current: std.json.Value = .{ .object = obj };
    var remaining = path;

    while (remaining.len > 0) {
        const dot_pos = std.mem.indexOf(u8, remaining, ".");
        const field = if (dot_pos) |pos| remaining[0..pos] else remaining;
        remaining = if (dot_pos) |pos| remaining[pos + 1 ..] else "";

        switch (current) {
            .object => |o| {
                if (o.get(field)) |val| {
                    current = val;
                } else {
                    return null;
                }
            },
            else => return null,
        }
    }

    return current;
}

/// Case-insensitive substring search.
fn containsLower(haystack: []const u8, needle_lower: []const u8) bool {
    if (needle_lower.len == 0) return true;
    if (haystack.len < needle_lower.len) return false;
    const end = haystack.len - needle_lower.len + 1;
    outer: for (0..end) |i| {
        for (0..needle_lower.len) |j| {
            if (std.ascii.toLower(haystack[i + j]) != needle_lower[j]) continue :outer;
        }
        return true;
    }
    return false;
}

// =============================================================================
// Tests
// =============================================================================

test "handleWorkflowRequest routes definitions" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleWorkflowRequest(allocator, .GET, "/definitions", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "handleWorkflowRequest routes runs" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleWorkflowRequest(allocator, .GET, "/runs", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "handleWorkflowRequest get definition detail" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleWorkflowRequest(allocator, .GET, "/definitions/my-workflow", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"my-workflow\"") != null);
}

test "handleWorkflowRequest cancel run" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleWorkflowRequest(allocator, .DELETE, "/runs/run-123", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"run_id\":\"run-123\"") != null);
}
