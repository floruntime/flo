//! Dashboard API — Workflow Endpoints
//!
//! Handles workflow REST API through the dashboard server (port 9002) which
//! has access to ALL dispatchers. Uses routeToShard() to dispatch commands
//! to the correct shard, avoiding the SO_REUSEPORT cross-shard issue on
//! port 9000 where HTTP handlers can only return synchronous results.
//!
//! Endpoints:
//!   GET    /workflow/:ns/runs                         — List workflow runs
//!   POST   /workflow/:ns/runs                         — Start a new run
//!   GET    /workflow/:ns/runs/:run_id                 — Get run status
//!   GET    /workflow/:ns/runs/:run_id/history         — Get run history
//!   POST   /workflow/:ns/runs/:run_id/cancel          — Cancel a run
//!   POST   /workflow/:ns/runs/:run_id/signal          — Signal a run
//!   GET    /workflow/:ns/definitions                   — List definitions
//!   POST   /workflow/:ns/definitions                   — Create definition
//!   GET    /workflow/:ns/definitions/:name             — Get definition YAML
//!   POST   /workflow/:ns/definitions/:name/enable      — Enable workflow
//!   POST   /workflow/:ns/definitions/:name/disable     — Disable workflow

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const Dispatcher = h.Dispatcher;
const Command = h.Command;
const Method = @import("../../../util/http/mod.zig").Method;

// =============================================================================
// Workflow Runs
// =============================================================================

/// GET /workflow/:ns/runs — List workflow runs with optional filters
pub fn listRuns(allocator: Allocator, namespace: []const u8, query_string: ?[]const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try allocator.dupe(u8, "[]");

    // Parse filters
    const workflow_name = h.parseQueryParam([]const u8, query_string, "workflow") orelse "";
    const status_filter = h.parseQueryParam([]const u8, query_string, "status");
    const limit = h.parseQueryParam(u32, query_string, "limit") orelse 100;

    // Route to the shard that owns this workflow name (or hash("", namespace) for all)
    const target = h.routeToShard(dispatchers, namespace, workflow_name);

    const result = target.dispatch(.{
        .workflow_list_runs = .{
            .namespace = namespace,
            .workflow_name = workflow_name,
            .status_filter = status_filter,
            .limit = limit,
            .cursor = null,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_list_runs_result => |r| return try allocator.dupe(u8, r.data),
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try allocator.dupe(u8, "[]");
}

/// POST /workflow/:ns/runs — Start a new workflow run
pub fn startRun(allocator: Allocator, namespace: []const u8, body: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try errJson(allocator, "No dispatchers available");

    // Parse JSON body
    const parsed = std.json.parseFromSlice(struct {
        workflow: []const u8,
        version: []const u8 = "latest",
        input: []const u8 = "{}",
        run_id: ?[]const u8 = null,
        idempotency_key: ?[]const u8 = null,
    }, allocator, body, .{}) catch {
        return try errJson(allocator, "Invalid JSON body");
    };
    defer parsed.deinit();
    const req = parsed.value;

    const route_key = if (req.run_id) |rid| rid else req.workflow;
    const target = h.routeToShard(dispatchers, namespace, route_key);

    const result = target.dispatch(.{
        .workflow_start = .{
            .namespace = namespace,
            .workflow_name = req.workflow,
            .workflow_version = req.version,
            .input = req.input,
            .idempotency_key = req.idempotency_key,
            .run_id = req.run_id,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_started => |r| {
                var buf = std.ArrayList(u8){};
                errdefer buf.deinit(allocator);
                const w = buf.writer(allocator);
                try w.print("{{\"run_id\":\"{s}\",\"already_exists\":{s}}}", .{ r.run_id, if (r.already_exists) "true" else "false" });
                return try buf.toOwnedSlice(allocator);
            },
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try errJson(allocator, "Request could not be processed");
}

/// GET /workflow/:ns/runs/:run_id — Get run status
pub fn getRunStatus(allocator: Allocator, namespace: []const u8, run_id: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try errJson(allocator, "No dispatchers available");

    const target = h.routeToShard(dispatchers, namespace, run_id);

    const result = target.dispatch(.{
        .workflow_status = .{
            .namespace = namespace,
            .run_id = run_id,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_status_result => |r| return try allocator.dupe(u8, r.data),
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try errJson(allocator, "Run not found");
}

/// GET /workflow/:ns/runs/:run_id/history — Get run event history
pub fn getRunHistory(allocator: Allocator, namespace: []const u8, run_id: []const u8, query_string: ?[]const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try allocator.dupe(u8, "[]");

    const limit = h.parseQueryParam(u32, query_string, "limit") orelse 100;

    const target = h.routeToShard(dispatchers, namespace, run_id);

    const result = target.dispatch(.{
        .workflow_history = .{
            .namespace = namespace,
            .run_id = run_id,
            .limit = limit,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_history_result => |r| return try allocator.dupe(u8, r.data),
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try allocator.dupe(u8, "[]");
}

/// POST /workflow/:ns/runs/:run_id/cancel — Cancel a run
pub fn cancelRun(allocator: Allocator, namespace: []const u8, run_id: []const u8, body: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try errJson(allocator, "No dispatchers available");

    // Parse reason from body (if present). Keep parsed alive until after dispatch.
    const parsed = if (body.len > 0) std.json.parseFromSlice(
        struct { reason: []const u8 = "Cancelled by user" },
        allocator,
        body,
        .{},
    ) catch null else null;
    defer if (parsed) |p| p.deinit();

    const reason: ?[]const u8 = if (parsed) |p| p.value.reason else "Cancelled by user";

    const target = h.routeToShard(dispatchers, namespace, run_id);

    const result = target.dispatch(.{
        .workflow_cancel = .{
            .namespace = namespace,
            .run_id = run_id,
            .reason = reason,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_cancelled => return try allocator.dupe(u8, "{\"ok\":true}"),
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try errJson(allocator, "Request could not be processed");
}

/// POST /workflow/:ns/runs/:run_id/signal — Signal a run
pub fn signalRun(allocator: Allocator, namespace: []const u8, run_id: []const u8, body: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try errJson(allocator, "No dispatchers available");

    const parsed = std.json.parseFromSlice(struct {
        signal_type: []const u8,
        payload: []const u8 = "{}",
    }, allocator, body, .{}) catch {
        return try errJson(allocator, "Invalid JSON body — expected {\"signal_type\":\"...\"}");
    };
    defer parsed.deinit();
    const req = parsed.value;

    const target = h.routeToShard(dispatchers, namespace, run_id);

    const result = target.dispatch(.{
        .workflow_signal = .{
            .namespace = namespace,
            .run_id = run_id,
            .signal_type = req.signal_type,
            .payload = req.payload,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_signaled => return try allocator.dupe(u8, "{\"ok\":true}"),
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try errJson(allocator, "Request could not be processed");
}

// =============================================================================
// Workflow Definitions
// =============================================================================

/// GET /workflow/:ns/definitions — List workflow definitions
pub fn listDefinitions(allocator: Allocator, namespace: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try allocator.dupe(u8, "[]");

    // list_definitions routing key: hashKey(namespace) — route via routeToShard with empty key
    const target = h.routeToShard(dispatchers, namespace, "");

    const result = target.dispatch(.{
        .workflow_list_definitions = .{
            .namespace = namespace,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_list_definitions_result => |r| return try allocator.dupe(u8, r.data),
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try allocator.dupe(u8, "[]");
}

/// POST /workflow/:ns/definitions — Create a workflow definition
pub fn createDefinition(allocator: Allocator, namespace: []const u8, body: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try errJson(allocator, "No dispatchers available");

    // workflow_create routes to shard 0 (null routing key)
    const result = dispatchers[0].dispatch(.{
        .workflow_create = .{
            .namespace = namespace,
            .definition_yaml = body,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_created => |r| {
                var buf = std.ArrayList(u8){};
                errdefer buf.deinit(allocator);
                const w = buf.writer(allocator);
                try w.print("{{\"workflow_name\":\"{s}\"}}", .{r.workflow_name});
                return try buf.toOwnedSlice(allocator);
            },
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try errJson(allocator, "Request could not be processed");
}

/// GET /workflow/:ns/definitions/:name — Get definition YAML
pub fn getDefinition(allocator: Allocator, namespace: []const u8, name: []const u8, query_string: ?[]const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try errJson(allocator, "No dispatchers available");

    const version = h.parseQueryParam([]const u8, query_string, "version");

    const target = h.routeToShard(dispatchers, namespace, name);

    const result = target.dispatch(.{
        .workflow_get_definition = .{
            .namespace = namespace,
            .workflow_name = name,
            .version = version,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_definition_result => |r| {
                var buf = std.ArrayList(u8){};
                errdefer buf.deinit(allocator);
                const w = buf.writer(allocator);
                var obj = h.json.ObjectBuilder(@TypeOf(w)).init(w);
                try obj.begin();
                try obj.stringField("definition_yaml", r.definition_yaml);
                try obj.end();
                return try buf.toOwnedSlice(allocator);
            },
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try errJson(allocator, "Definition not found");
}

/// POST /workflow/:ns/definitions/:name/enable — Enable a workflow
pub fn enableWorkflow(allocator: Allocator, namespace: []const u8, name: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try errJson(allocator, "No dispatchers available");

    const target = h.routeToShard(dispatchers, namespace, name);

    const result = target.dispatch(.{
        .workflow_enable = .{
            .namespace = namespace,
            .workflow_name = name,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_enabled => return try allocator.dupe(u8, "{\"ok\":true}"),
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try errJson(allocator, "Request could not be processed");
}

/// POST /workflow/:ns/definitions/:name/disable — Disable a workflow
pub fn disableWorkflow(allocator: Allocator, namespace: []const u8, name: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try errJson(allocator, "No dispatchers available");

    const target = h.routeToShard(dispatchers, namespace, name);

    const result = target.dispatch(.{
        .workflow_disable = .{
            .namespace = namespace,
            .workflow_name = name,
        },
    }, 0, 0, null) catch |err| {
        return try errJson(allocator, @errorName(err));
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .workflow_disabled => return try allocator.dupe(u8, "{\"ok\":true}"),
            .err => |e| return try errJson(allocator, e.message),
            else => return try errJson(allocator, "Unexpected result"),
        }
    }

    return try errJson(allocator, "Request could not be processed");
}

// =============================================================================
// Workflow Router — dispatches method + sub-path to the right handler
// =============================================================================

/// Route a workflow API request: path is after `workflow/`, e.g. `default/runs`
pub fn handleWorkflowRequest(
    allocator: Allocator,
    method: Method,
    path: []const u8,
    query_string: ?[]const u8,
    body: []const u8,
    dispatchers: []*Dispatcher,
) ![]const u8 {
    // path = ":namespace/..." — extract namespace
    const ns_end = std.mem.indexOf(u8, path, "/") orelse {
        return try errJson(allocator, "Missing resource path after namespace");
    };
    const namespace = path[0..ns_end];
    const rest = path[ns_end + 1 ..];

    // rest = "runs" | "runs/:id" | "runs/:id/..." | "definitions" | "definitions/:name" | "definitions/:name/..."
    if (std.mem.eql(u8, rest, "runs")) {
        return switch (method) {
            .GET => try listRuns(allocator, namespace, query_string, dispatchers),
            .POST => try startRun(allocator, namespace, body, dispatchers),
            else => try errJson(allocator, "Method not allowed"),
        };
    }

    if (std.mem.startsWith(u8, rest, "runs/")) {
        const runs_rest = rest["runs/".len..];
        // runs_rest = ":run_id" | ":run_id/history" | ":run_id/cancel" | ":run_id/signal"
        if (std.mem.indexOf(u8, runs_rest, "/")) |slash| {
            const run_id = runs_rest[0..slash];
            const sub = runs_rest[slash + 1 ..];
            if (std.mem.eql(u8, sub, "history")) {
                return try getRunHistory(allocator, namespace, run_id, query_string, dispatchers);
            } else if (std.mem.eql(u8, sub, "cancel")) {
                return try cancelRun(allocator, namespace, run_id, body, dispatchers);
            } else if (std.mem.eql(u8, sub, "signal")) {
                return try signalRun(allocator, namespace, run_id, body, dispatchers);
            } else {
                return try errJson(allocator, "Unknown run sub-resource");
            }
        } else {
            // GET /workflow/:ns/runs/:run_id
            return try getRunStatus(allocator, namespace, runs_rest, dispatchers);
        }
    }

    if (std.mem.eql(u8, rest, "definitions")) {
        return switch (method) {
            .GET => try listDefinitions(allocator, namespace, dispatchers),
            .POST => try createDefinition(allocator, namespace, body, dispatchers),
            else => try errJson(allocator, "Method not allowed"),
        };
    }

    if (std.mem.startsWith(u8, rest, "definitions/")) {
        const defs_rest = rest["definitions/".len..];
        if (std.mem.indexOf(u8, defs_rest, "/")) |slash| {
            const name = defs_rest[0..slash];
            const sub = defs_rest[slash + 1 ..];
            if (std.mem.eql(u8, sub, "enable")) {
                return try enableWorkflow(allocator, namespace, name, dispatchers);
            } else if (std.mem.eql(u8, sub, "disable")) {
                return try disableWorkflow(allocator, namespace, name, dispatchers);
            } else {
                return try errJson(allocator, "Unknown definition sub-resource");
            }
        } else {
            return try getDefinition(allocator, namespace, defs_rest, query_string, dispatchers);
        }
    }

    return try errJson(allocator, "Unknown workflow endpoint");
}

// =============================================================================
// Helpers
// =============================================================================

fn errJson(allocator: Allocator, message: []const u8) ![]const u8 {
    return try h.jsonError(allocator, message);
}
