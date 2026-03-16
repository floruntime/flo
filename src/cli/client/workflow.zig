//! Workflow Client Operations
//!
//! Workflow operations for the Flo CLI client.
//! Uses the Request format: namespace, key, value, options
//!
//! Wire format conventions (from conversion.zig):
//!   - namespace: always req.namespace
//!   - key: varies by command (run_id, workflow_name, etc.)
//!   - value: serialized parameters

const std = @import("std");
const base = @import("base.zig");
const wire = @import("../../util/wire.zig");
const Client = base.Client;
const Response = base.Response;
const proto = @import("../../protocol/proto.zig");
const FixedWireWriter = wire.FixedWireWriter;

// =============================================================================
// Workflow Operations
// =============================================================================

/// Create a workflow definition from YAML
/// Wire format:
///   - namespace: req.namespace
///   - key: workflow name (extracted client-side from YAML for routing)
///   - value: definition_yaml
pub fn create(
    client: *Client,
    namespace: []const u8,
    name: []const u8,
    definition_yaml: []const u8,
) !Response {
    return client.sendRequest(.workflow_create, namespace, name, definition_yaml);
}

/// Start a workflow run
/// Wire format in value:
///   [version_len:u16][version][input][has_idempotency_key:u8][key_len:u16]?[key]?[has_run_id:u8][run_id_len:u16]?[run_id]?
///
/// key = workflow_name
pub fn start(
    client: *Client,
    namespace: []const u8,
    workflow_name: []const u8,
    workflow_version: []const u8,
    input: []const u8,
    idempotency_key: ?[]const u8,
    run_id: ?[]const u8,
) !Response {
    var value_buf: [8192]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write version with length prefix
    try writer.writeInt(u16, @intCast(workflow_version.len), .little);
    try writer.writeAll(workflow_version);

    // Write input (rest will be input data)
    // But we also need optional fields, so encode them first
    // Format: [version_len:u16][version][has_idempotency:u8][key_len:u16]?[key]?[has_run_id:u8][run_id_len:u16]?[run_id]?[input...]

    // Write optional idempotency_key
    if (idempotency_key) |key| {
        try writer.writeByte(1);
        try writer.writeInt(u16, @intCast(key.len), .little);
        try writer.writeAll(key);
    } else {
        try writer.writeByte(0);
    }

    // Write optional run_id
    if (run_id) |rid| {
        try writer.writeByte(1);
        try writer.writeInt(u16, @intCast(rid.len), .little);
        try writer.writeAll(rid);
    } else {
        try writer.writeByte(0);
    }

    // Write input
    try writer.writeAll(input);

    const value = fbs.getWritten();
    return client.sendRequest(.workflow_start, namespace, workflow_name, value);
}

/// Send a signal to a running workflow
/// Wire format in value:
///   [signal_len:u16][signal_type][payload...]
///
/// key = run_id
pub fn signal(
    client: *Client,
    namespace: []const u8,
    run_id: []const u8,
    signal_type: []const u8,
    payload: ?[]const u8,
) !Response {
    var value_buf: [8192]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write signal_type with length prefix
    try writer.writeInt(u16, @intCast(signal_type.len), .little);
    try writer.writeAll(signal_type);

    // Write payload if present
    if (payload) |p| {
        try writer.writeAll(p);
    }

    const value = fbs.getWritten();
    return client.sendRequest(.workflow_signal, namespace, run_id, value);
}

/// Cancel a running workflow
/// Wire format:
///   - namespace: req.namespace
///   - key: run_id
///   - value: reason (optional)
pub fn cancel(
    client: *Client,
    namespace: []const u8,
    run_id: []const u8,
    reason: ?[]const u8,
) !Response {
    return client.sendRequest(.workflow_cancel, namespace, run_id, reason orelse "");
}

/// Get workflow run status
/// Wire format:
///   - namespace: req.namespace
///   - key: run_id
///   - value: (empty)
pub fn status(
    client: *Client,
    namespace: []const u8,
    run_id: []const u8,
) !Response {
    return client.sendRequest(.workflow_status, namespace, run_id, "");
}

/// Get workflow run history
/// Wire format in value:
///   [limit:u32]
///
/// key = run_id
pub fn history(
    client: *Client,
    namespace: []const u8,
    run_id: []const u8,
    limit: u32,
) !Response {
    var value_buf: [8]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    try writer.writeInt(u32, limit, .little);

    const value = fbs.getWritten();
    return client.sendRequest(.workflow_history, namespace, run_id, value);
}

/// List workflow runs
/// Wire format in value:
///   [limit:u32][status_filter_len:u16]?[status_filter]?[cursor_len:u16]?[cursor]?
///
/// key = workflow_name
pub fn listRuns(
    client: *Client,
    namespace: []const u8,
    workflow_name: []const u8,
    limit: u32,
    status_filter: ?[]const u8,
    cursor: ?[]const u8,
) !Response {
    var value_buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write limit
    try writer.writeInt(u32, limit, .little);

    // Write optional status_filter
    if (status_filter) |sf| {
        try writer.writeInt(u16, @intCast(sf.len), .little);
        try writer.writeAll(sf);
    } else {
        try writer.writeInt(u16, 0, .little);
    }

    // Write optional cursor
    if (cursor) |c| {
        try writer.writeInt(u16, @intCast(c.len), .little);
        try writer.writeAll(c);
    } else {
        try writer.writeInt(u16, 0, .little);
    }

    const value = fbs.getWritten();
    return client.sendRequest(.workflow_list_runs, namespace, workflow_name, value);
}

/// Get workflow definition
/// Wire format:
///   - namespace: req.namespace
///   - key: workflow_name
///   - value: version (optional)
pub fn getDefinition(
    client: *Client,
    namespace: []const u8,
    workflow_name: []const u8,
    version: ?[]const u8,
) !Response {
    return client.sendRequest(.workflow_get_definition, namespace, workflow_name, version orelse "");
}

/// Disable a workflow (blocks new runs, pauses schedules)
/// Wire format:
///   - namespace: req.namespace
///   - key: workflow_name
///   - value: version (optional)
pub fn disable(
    client: *Client,
    namespace: []const u8,
    workflow_name: []const u8,
    version: ?[]const u8,
) !Response {
    return client.sendRequest(.workflow_disable, namespace, workflow_name, version orelse "");
}

/// Enable a workflow (allows runs, resumes schedules)
/// Wire format:
///   - namespace: req.namespace
///   - key: workflow_name
///   - value: version (optional)
pub fn enable(
    client: *Client,
    namespace: []const u8,
    workflow_name: []const u8,
    version: ?[]const u8,
) !Response {
    return client.sendRequest(.workflow_enable, namespace, workflow_name, version orelse "");
}

/// List workflow definitions
/// Wire format:
///   - namespace: req.namespace
///   - key: (unused)
///   - value: (empty)
pub fn listDefinitions(
    client: *Client,
    namespace: []const u8,
) !Response {
    return client.sendRequest(.workflow_list_definitions, namespace, "", "");
}
