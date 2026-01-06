//! Dashboard API — Actions & Workers Endpoints (Layer 2 — Intelligent Layer)
//!
//! - GET /actions                    — All registered actions
//! - GET /actions/:name              — Action detail with runs and workers
//! - GET /actions/:name/runs         — List runs for an action
//! - GET /actions/:name/invoke       — Trigger an action from dashboard
//! - GET /workers                    — All registered workers

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const Dispatcher = h.Dispatcher;
const Core = h.Core;
const ActionMeta = h.ActionMeta;
const ActionRun = h.ActionRun;
const WorkerMeta = h.WorkerMeta;
const DiscoveredAction = h.DiscoveredAction;
const DiscoveredWorker = h.DiscoveredWorker;
const DiscoveredRun = h.DiscoveredRun;
const RunCounts = h.RunCounts;

// =============================================================================
// State Engine Scanners (action-domain specific)
// =============================================================================

/// Scan all cores' state engines for actions.
/// Key format in state engine: "ns:{namespace}:kv:_action:{name}" (KV layer adds ns:...:kv: prefix)
fn scanActionsFromCores(allocator: Allocator, cores: ?[]*Core) ![]DiscoveredAction {
    const c = cores orelse return try allocator.alloc(DiscoveredAction, 0);
    if (c.len == 0) return try allocator.alloc(DiscoveredAction, 0);

    var actions: std.ArrayList(DiscoveredAction) = .empty;
    errdefer {
        for (actions.items) |a| freeOneAction(allocator, a);
        actions.deinit(allocator);
    }

    for (c) |core| {
        const state = core.state_engine;
        var iter = state.scan("ns:");

        while (iter.next()) |entry| {
            const key = entry.key;

            // Keys are stored as: ns:{namespace}:kv:_action:{name}
            // Find the :kv: separator to split namespace from entity key
            const kv_marker = ":kv:";
            const kv_pos = std.mem.indexOf(u8, key, kv_marker) orelse continue;

            const entity_key = key[kv_pos + kv_marker.len ..];

            // Match _action:{name} but exclude _action_run: keys
            if (!std.mem.startsWith(u8, entity_key, "_action:")) continue;
            if (std.mem.startsWith(u8, entity_key, "_action_run:")) continue;

            const ns_start: usize = 3; // skip "ns:"
            if (key.len < ns_start) continue;
            const namespace = key[ns_start..kv_pos];
            if (namespace.len == 0) continue;

            const name = entity_key["_action:".len..];
            if (name.len == 0) continue;

            var found = false;
            for (actions.items) |a| {
                if (std.mem.eql(u8, a.name, name) and std.mem.eql(u8, a.namespace, namespace)) {
                    found = true;
                    break;
                }
            }
            if (found) continue;

            var meta = ActionMeta.decode(allocator, entry.value) catch {
                try actions.append(allocator, .{
                    .name = try allocator.dupe(u8, name),
                    .namespace = try allocator.dupe(u8, namespace),
                    .action_type = try allocator.dupe(u8, "unknown"),
                    .owner = try allocator.dupe(u8, ""),
                    .description = try allocator.dupe(u8, ""),
                    .version = try allocator.dupe(u8, ""),
                    .enabled = true,
                    .timeout_ms = 30000,
                    .max_retries = 3,
                    .retry_delay_ms = 1000,
                    .trigger_stream = try allocator.dupe(u8, ""),
                    .trigger_group = try allocator.dupe(u8, ""),
                    .created_at = 0,
                    .updated_at = 0,
                });
                continue;
            };
            defer meta.deinit(allocator);

            try actions.append(allocator, .{
                .name = try allocator.dupe(u8, meta.name),
                .namespace = try allocator.dupe(u8, meta.namespace),
                .action_type = try allocator.dupe(u8, if (meta.action_type == .wasm) "wasm" else "user"),
                .owner = try allocator.dupe(u8, meta.owner),
                .description = try allocator.dupe(u8, meta.description orelse ""),
                .version = try allocator.dupe(u8, meta.version),
                .enabled = meta.enabled,
                .timeout_ms = meta.timeout_ms,
                .max_retries = meta.max_retries,
                .retry_delay_ms = meta.retry_delay_ms,
                .trigger_stream = try allocator.dupe(u8, meta.trigger_stream orelse ""),
                .trigger_group = try allocator.dupe(u8, meta.trigger_group orelse ""),
                .created_at = meta.created_at,
                .updated_at = meta.updated_at,
            });
        }
    }

    return try actions.toOwnedSlice(allocator);
}

fn freeOneAction(allocator: Allocator, a: DiscoveredAction) void {
    allocator.free(a.name);
    allocator.free(a.namespace);
    allocator.free(a.action_type);
    allocator.free(a.owner);
    allocator.free(a.description);
    allocator.free(a.version);
    allocator.free(a.trigger_stream);
    allocator.free(a.trigger_group);
}

/// Scan all cores' state engines for workers.
fn scanWorkersFromCores(allocator: Allocator, cores: ?[]*Core) ![]DiscoveredWorker {
    const c = cores orelse return try allocator.alloc(DiscoveredWorker, 0);
    if (c.len == 0) return try allocator.alloc(DiscoveredWorker, 0);

    var workers: std.ArrayList(DiscoveredWorker) = .empty;
    errdefer {
        for (workers.items) |w| {
            allocator.free(w.worker_id);
            allocator.free(w.namespace);
            allocator.free(w.task_types);
        }
        workers.deinit(allocator);
    }

    for (c) |core| {
        const state = core.state_engine;
        var iter = state.scan("ns:");

        while (iter.next()) |entry| {
            const key = entry.key;

            // Keys are stored as: ns:{namespace}:kv:_worker:{worker_id}
            const kv_marker = ":kv:";
            const kv_pos = std.mem.indexOf(u8, key, kv_marker) orelse continue;

            const entity_key = key[kv_pos + kv_marker.len ..];

            // Match _worker:{id} but exclude _worker_task: keys
            if (!std.mem.startsWith(u8, entity_key, "_worker:")) continue;
            if (std.mem.startsWith(u8, entity_key, "_worker_task:")) continue;

            const ns_start: usize = 3;
            if (key.len < ns_start) continue;
            const namespace = key[ns_start..kv_pos];
            if (namespace.len == 0) continue;

            const worker_id = entity_key["_worker:".len..];
            if (worker_id.len == 0) continue;

            var found = false;
            for (workers.items) |w| {
                if (std.mem.eql(u8, w.worker_id, worker_id) and std.mem.eql(u8, w.namespace, namespace)) {
                    found = true;
                    break;
                }
            }
            if (found) continue;

            var wmeta = WorkerMeta.decode(allocator, entry.value) catch {
                try workers.append(allocator, .{
                    .worker_id = try allocator.dupe(u8, worker_id),
                    .namespace = try allocator.dupe(u8, namespace),
                    .task_types = try allocator.dupe(u8, ""),
                    .healthy = false,
                    .current_load = 0,
                    .max_concurrent = 0,
                    .active_tasks = 0,
                    .last_seen = 0,
                    .registered_at = 0,
                });
                continue;
            };
            defer wmeta.deinit(allocator);

            var types_buf: std.ArrayList(u8) = .empty;
            for (wmeta.task_types, 0..) |tt, i| {
                if (i > 0) types_buf.appendSlice(allocator, ",") catch {};
                types_buf.appendSlice(allocator, tt) catch {};
            }
            const task_types_str = types_buf.toOwnedSlice(allocator) catch try allocator.dupe(u8, "");

            try workers.append(allocator, .{
                .worker_id = try allocator.dupe(u8, wmeta.worker_id),
                .namespace = try allocator.dupe(u8, wmeta.namespace),
                .task_types = task_types_str,
                .healthy = wmeta.healthy,
                .current_load = wmeta.current_load,
                .max_concurrent = wmeta.max_concurrent,
                .active_tasks = wmeta.active_tasks,
                .last_seen = wmeta.last_seen,
                .registered_at = wmeta.registered_at,
            });
        }
    }

    return try workers.toOwnedSlice(allocator);
}

/// Count action runs by status
fn countActionRuns(allocator: Allocator, cores: ?[]*Core, namespace: []const u8, action_name: []const u8) RunCounts {
    const c = cores orelse return .{};
    if (c.len == 0) return .{};

    var counts = RunCounts{};

    for (c) |core| {
        const state = core.state_engine;
        var prefix_buf: [256]u8 = undefined;
        const prefix = std.fmt.bufPrint(&prefix_buf, "ns:{s}:kv:_action_run:{s}-", .{ namespace, action_name }) catch continue;
        var iter = state.scan(prefix);

        while (iter.next()) |entry| {
            counts.total += 1;

            var run = ActionRun.decode(allocator, entry.value) catch continue;
            defer run.deinit(allocator);

            switch (run.status) {
                .pending => counts.pending += 1,
                .running => counts.running += 1,
                .completed => counts.completed += 1,
                .failed => counts.failed += 1,
                .cancelled => counts.cancelled += 1,
                .timed_out => counts.timed_out += 1,
            }
        }
    }

    return counts;
}

/// Count workers for a specific action
fn countWorkersForAction(cores: ?[]*Core, namespace: []const u8, action_name: []const u8) u32 {
    const c = cores orelse return 0;
    if (c.len == 0) return 0;

    var count: u32 = 0;
    var seen_buf: [64][]const u8 = undefined;
    var seen_count: usize = 0;

    for (c) |core| {
        const state = core.state_engine;
        var prefix_buf: [256]u8 = undefined;
        const prefix = std.fmt.bufPrint(&prefix_buf, "ns:{s}:kv:_worker_task:{s}:", .{ namespace, action_name }) catch continue;
        var iter = state.scan(prefix);

        while (iter.next()) |entry| {
            const key = entry.key;
            const last_colon = std.mem.lastIndexOf(u8, key, ":") orelse continue;
            const wid = key[last_colon + 1 ..];
            if (wid.len == 0) continue;

            var found = false;
            for (seen_buf[0..seen_count]) |s| {
                if (std.mem.eql(u8, s, wid)) {
                    found = true;
                    break;
                }
            }
            if (!found and seen_count < seen_buf.len) {
                seen_buf[seen_count] = wid;
                seen_count += 1;
                count += 1;
            }
        }
    }

    return count;
}

/// Scan recent runs for an action
fn scanRecentRuns(allocator: Allocator, cores: ?[]*Core, namespace: []const u8, action_name: []const u8, limit: u32) ![]DiscoveredRun {
    const c = cores orelse return try allocator.alloc(DiscoveredRun, 0);
    if (c.len == 0) return try allocator.alloc(DiscoveredRun, 0);

    var runs: std.ArrayList(DiscoveredRun) = .empty;
    errdefer {
        for (runs.items) |r| freeOneRun(allocator, r);
        runs.deinit(allocator);
    }

    for (c) |core| {
        const state = core.state_engine;
        var prefix_buf: [256]u8 = undefined;
        const prefix = std.fmt.bufPrint(&prefix_buf, "ns:{s}:kv:_action_run:{s}-", .{ namespace, action_name }) catch continue;
        var iter = state.scan(prefix);

        while (iter.next()) |entry| {
            if (runs.items.len >= limit) break;

            var run = ActionRun.decode(allocator, entry.value) catch continue;
            defer run.deinit(allocator);

            var found = false;
            for (runs.items) |r| {
                if (std.mem.eql(u8, r.run_id, run.run_id)) {
                    found = true;
                    break;
                }
            }
            if (found) continue;

            try runs.append(allocator, .{
                .run_id = try allocator.dupe(u8, run.run_id),
                .status = try allocator.dupe(u8, run.status.toString()),
                .attempt = run.attempt,
                .created_at = run.created_at,
                .started_at = run.started_at orelse 0,
                .completed_at = run.completed_at orelse 0,
                .worker_id = if (run.worker_id) |wid| try allocator.dupe(u8, wid) else try allocator.dupe(u8, ""),
                .error_message = if (run.error_message) |em| try allocator.dupe(u8, em) else try allocator.dupe(u8, ""),
                .outcome = try allocator.dupe(u8, run.outcome.toString()),
            });
        }
    }

    return try runs.toOwnedSlice(allocator);
}

fn freeOneRun(allocator: Allocator, r: DiscoveredRun) void {
    allocator.free(r.run_id);
    allocator.free(r.status);
    if (r.worker_id.len > 0) allocator.free(r.worker_id);
    if (r.error_message.len > 0) allocator.free(r.error_message);
    if (r.outcome.len > 0) allocator.free(r.outcome);
}

// =============================================================================
// HTTP Handlers
// =============================================================================

/// GET /actions - List all registered actions with rich metadata
pub fn getActions(allocator: Allocator, dispatchers: []*Dispatcher, cores: ?[]*Core) ![]const u8 {
    _ = dispatchers;

    const discovered = try scanActionsFromCores(allocator, cores);
    defer h.freeDiscoveredActions(allocator, discovered);

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    for (discovered) |action| {
        const run_counts = countActionRuns(allocator, cores, action.namespace, action.name);
        const worker_count = countWorkersForAction(cores, action.namespace, action.name);

        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", action.name);
        try obj.stringField("namespace", action.namespace);
        try obj.stringField("type", action.action_type);
        try obj.stringField("owner", action.owner);
        try obj.stringField("description", action.description);
        try obj.stringField("version", action.version);
        try obj.boolField("enabled", action.enabled);
        try obj.intField("timeout_ms", action.timeout_ms);
        try obj.intField("max_retries", action.max_retries);
        try obj.intField("created_at", action.created_at);
        try obj.intField("updated_at", action.updated_at);

        if (action.trigger_stream.len > 0) {
            try obj.stringField("trigger_stream", action.trigger_stream);
        }
        if (action.trigger_group.len > 0) {
            try obj.stringField("trigger_group", action.trigger_group);
        }

        try h.writeRunCountsJson(&obj, run_counts);
        try obj.intField("worker_count", worker_count);
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /actions/:name - Detailed action info with runs and workers
pub fn getActionDetail(allocator: Allocator, action_name: []const u8, dispatchers: []*Dispatcher, cores: ?[]*Core) ![]const u8 {
    _ = dispatchers;

    const discovered = try scanActionsFromCores(allocator, cores);
    defer h.freeDiscoveredActions(allocator, discovered);

    var action: ?DiscoveredAction = null;
    for (discovered) |a| {
        if (std.mem.eql(u8, a.name, action_name)) {
            action = a;
            break;
        }
    }

    const act = action orelse return try h.jsonError(allocator, "Action not found");

    const run_counts = countActionRuns(allocator, cores, act.namespace, act.name);

    const all_workers = try scanWorkersFromCores(allocator, cores);
    defer h.freeDiscoveredWorkers(allocator, all_workers);

    const recent_runs = try scanRecentRuns(allocator, cores, act.namespace, act.name, 20);
    defer h.freeDiscoveredRuns(allocator, recent_runs);

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();

    try obj.stringField("name", act.name);
    try obj.stringField("namespace", act.namespace);
    try obj.stringField("type", act.action_type);
    try obj.stringField("owner", act.owner);
    try obj.stringField("description", act.description);
    try obj.stringField("version", act.version);
    try obj.boolField("enabled", act.enabled);
    try obj.intField("timeout_ms", act.timeout_ms);
    try obj.intField("max_retries", act.max_retries);
    try obj.intField("retry_delay_ms", act.retry_delay_ms);
    try obj.intField("created_at", act.created_at);
    try obj.intField("updated_at", act.updated_at);

    if (act.trigger_stream.len > 0) {
        try obj.stringField("trigger_stream", act.trigger_stream);
    }
    if (act.trigger_group.len > 0) {
        try obj.stringField("trigger_group", act.trigger_group);
    }

    try h.writeRunCountsJson(&obj, run_counts);

    // Recent runs
    var recent_arr = try obj.arrayField("recent_runs");
    try recent_arr.begin();
    for (recent_runs) |run| {
        try recent_arr.next();
        var robj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try robj.begin();
        try h.writeRunJson(&robj, run);
        try robj.end();
    }
    try recent_arr.end();

    // Workers handling this action
    var workers_arr = try obj.arrayField("workers");
    try workers_arr.begin();
    for (all_workers) |w| {
        if (std.mem.indexOf(u8, w.task_types, act.name) != null) {
            try workers_arr.next();
            var wobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try wobj.begin();
            try wobj.stringField("worker_id", w.worker_id);
            try wobj.stringField("task_types", w.task_types);
            try wobj.boolField("healthy", w.healthy);
            try wobj.intField("current_load", w.current_load);
            try wobj.intField("active_tasks", w.active_tasks);
            try wobj.intField("max_concurrent", w.max_concurrent);
            try wobj.intField("last_seen", w.last_seen);
            try wobj.end();
        }
    }
    try workers_arr.end();

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /actions/:name/runs - List runs for an action
pub fn getActionRuns(allocator: Allocator, action_name: []const u8, query_string: ?[]const u8, dispatchers: []*Dispatcher, cores: ?[]*Core) ![]const u8 {
    _ = dispatchers;
    const limit = h.parseQueryParam(u32, query_string, "limit") orelse 50;
    const namespace = "default"; // TODO: parse from query

    const runs = try scanRecentRuns(allocator, cores, namespace, action_name, limit);
    defer h.freeDiscoveredRuns(allocator, runs);

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    for (runs) |run| {
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try h.writeRunJson(&obj, run);
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /actions/:name/invoke - Trigger an action from the dashboard
pub fn invokeAction(allocator: Allocator, action_name: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    const result = dispatchers[0].dispatch(.{ .action_invoke = .{
        .namespace = "default",
        .action_name = action_name,
        .input = "{}",
        .caller_id = "dashboard",
        .priority = 0,
        .delay_ms = 0,
        .idempotency_key = null,
    } }, 0, 0, null) catch {
        return try h.jsonError(allocator, "Failed to invoke action");
    };

    if (result) |res| {
        switch (res) {
            .action_invoked => |inv| {
                var json_buf = std.ArrayList(u8){};
                errdefer json_buf.deinit(allocator);
                const writer = json_buf.writer(allocator);

                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("status", "invoked");
                try obj.stringField("run_id", inv.run_id);
                try obj.end();

                return try json_buf.toOwnedSlice(allocator);
            },
            .err => |e| return try h.jsonError(allocator, e.message),
            else => return try h.jsonError(allocator, "Unexpected response"),
        }
    }

    return try h.jsonError(allocator, "No response from dispatcher");
}

/// GET /workers - List all registered workers
pub fn getWorkers(allocator: Allocator, dispatchers: []*Dispatcher, cores: ?[]*Core) ![]const u8 {
    _ = dispatchers;

    const discovered = try scanWorkersFromCores(allocator, cores);
    defer h.freeDiscoveredWorkers(allocator, discovered);

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    for (discovered) |worker| {
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("worker_id", worker.worker_id);
        try obj.stringField("namespace", worker.namespace);
        try obj.stringField("task_types", worker.task_types);
        try obj.boolField("healthy", worker.healthy);
        try obj.intField("current_load", worker.current_load);
        try obj.intField("max_concurrent", worker.max_concurrent);
        try obj.intField("active_tasks", worker.active_tasks);
        try obj.intField("last_seen", worker.last_seen);
        try obj.intField("registered_at", worker.registered_at);
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}
