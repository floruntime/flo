//! Action & Worker Client Operations
//!
//! Action and worker operations for the Flo CLI client.
//! Uses the Request format: namespace, key, value, options
//!
//! Wire format conventions (from conversion.zig):
//!   - namespace: always req.namespace
//!   - key: varies by command (action_name, run_id, worker_id, etc.)
//!   - value: serialized parameters

const std = @import("std");
const base = @import("base.zig");
const wire = @import("../../util/wire.zig");
const Client = base.Client;
const Response = base.Response;
const proto = @import("../../protocol/proto.zig");
const FixedWireWriter = wire.FixedWireWriter;

// =============================================================================
// Action Operations
// =============================================================================

/// Register an action
/// Wire format in value:
///   [action_type:u8][timeout_ms:u32][max_retries:u32]
///   [has_desc:u8][desc_len:u16]?[desc]?
///   [has_wasm_module:u8]... etc
///
/// key = owner (used as req.key)
/// name passed first in value for actual action name
pub fn register(
    client: *Client,
    namespace: []const u8,
    action_name: []const u8,
    action_type: u8,
    _: []const u8, // owner - not used in current wire format
    timeout_ms: ?u32,
    max_retries: ?u8,
    _: ?u32, // retry_delay_ms - not in wire format
    wasm_module_bytes: ?[]const u8,
) !Response {
    // For WASM actions with module bytes, we need dynamic allocation
    // since the fixed buffer is too small.
    const wasm_len = if (wasm_module_bytes) |wb| wb.len else 0;
    const header_size: usize = 1 + 4 + 4 + 1 + 1 + (if (wasm_len > 0) @as(usize, 2 + wasm_len) else 0) + 1 + 1 + 1 + 1;

    // Use stack buffer for small payloads, heap for large (WASM)
    if (wasm_len > 0) {
        const alloc = client.allocator;
        const buf = try alloc.alloc(u8, header_size);
        defer alloc.free(buf);
        var fbs = std.io.fixedBufferStream(buf);
        const writer = fbs.writer();

        try writer.writeByte(action_type);
        try writer.writeInt(u32, timeout_ms orelse 30_000, .little);
        try writer.writeInt(u32, @as(u32, max_retries orelse 3), .little);
        try writer.writeByte(0); // no description

        // Write WASM module bytes
        try writer.writeByte(1); // has_wasm_module = 1
        try writer.writeInt(u16, @intCast(wasm_len), .little);
        try writer.writeAll(wasm_module_bytes.?);

        try writer.writeByte(0); // no wasm_entrypoint
        try writer.writeByte(0); // no wasm_memory_limit
        try writer.writeByte(0); // no trigger_stream
        try writer.writeByte(0); // no trigger_group

        return client.sendRequest(.action_register, namespace, action_name, fbs.getWritten());
    }

    // Small payload path (no WASM bytes)
    var value_buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    try writer.writeByte(action_type);
    try writer.writeInt(u32, timeout_ms orelse 30_000, .little);
    try writer.writeInt(u32, @as(u32, max_retries orelse 3), .little);
    try writer.writeByte(0); // no description
    try writer.writeByte(0); // no wasm_module
    try writer.writeByte(0); // no wasm_entrypoint
    try writer.writeByte(0); // no wasm_memory_limit
    try writer.writeByte(0); // no trigger_stream
    try writer.writeByte(0); // no trigger_group

    return client.sendRequest(.action_register, namespace, action_name, fbs.getWritten());
}

/// Invoke an action
/// Wire format in value:
///   [priority:u8][delay_ms:i64][has_caller:u8]...[input...]
/// key = action_name
pub fn invoke(
    client: *Client,
    namespace: []const u8,
    action_name: []const u8,
    input: []const u8,
    priority: ?u8,
    idempotency_key: ?[]const u8,
    required_labels: ?[]const u8,
) !Response {
    var value_buf: [8192]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write priority
    try writer.writeByte(priority orelse 10);

    // Write delay_ms (i64, default 0)
    try writer.writeInt(i64, 0, .little);

    // Write caller_id (optional, none)
    try writer.writeByte(0);

    // Write idempotency_key (optional)
    if (idempotency_key) |key| {
        try writer.writeByte(1);
        try writer.writeInt(u16, @intCast(key.len), .little);
        try writer.writeAll(key);
    } else {
        try writer.writeByte(0);
    }

    // Write required_labels (optional)
    if (required_labels) |labels| {
        try writer.writeByte(1);
        try writer.writeInt(u16, @intCast(labels.len), .little);
        try writer.writeAll(labels);
    } else {
        try writer.writeByte(0);
    }

    // Write input (rest of value)
    try writer.writeAll(input);

    const value = fbs.getWritten();
    return client.sendRequest(.action_invoke, namespace, action_name, value);
}

/// Get action run status
/// key = run_id
pub fn status(client: *Client, namespace: []const u8, run_id: []const u8) !Response {
    return client.sendRequest(.action_status, namespace, run_id, "");
}

/// List registered actions
/// key = prefix (or empty)
/// value = [limit:u32][cursor...]
pub fn list(
    client: *Client,
    namespace: []const u8,
    limit: ?u32,
    prefix: ?[]const u8,
    cursor: ?[]const u8,
) !Response {
    var value_buf: [256]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write limit
    try writer.writeInt(u32, limit orelse 100, .little);

    // Write cursor if provided (for shard walking)
    if (cursor) |c| {
        _ = writer.write(c) catch return error.BufferOverflow;
    }

    const value = fbs.getWritten();
    return client.sendRequest(.action_list, namespace, prefix orelse "", value);
}

/// Delete an action
/// key = action_name
pub fn delete(client: *Client, namespace: []const u8, action_name: []const u8) !Response {
    return client.sendRequest(.action_delete, namespace, action_name, "");
}

// =============================================================================
// Worker Operations
// =============================================================================

/// Register a worker
/// key = worker_id
/// value = [count:u32][task_type_len:u16][task_type]...[has_labels:u8][labels_len:u16][labels]?
pub fn workerRegister(
    client: *Client,
    namespace: []const u8,
    worker_id: []const u8,
    task_types: []const []const u8,
    labels: ?[]const u8,
) !Response {
    var value_buf: [4096]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write task_types count
    try writer.writeInt(u32, @intCast(task_types.len), .little);

    // Write each task type
    for (task_types) |tt| {
        try writer.writeInt(u16, @intCast(tt.len), .little);
        try writer.writeAll(tt);
    }

    // Write labels (optional JSON string)
    if (labels) |l| {
        try writer.writeByte(1);
        try writer.writeInt(u16, @intCast(l.len), .little);
        try writer.writeAll(l);
    } else {
        try writer.writeByte(0);
    }

    const value = fbs.getWritten();
    return client.sendRequest(.worker_register, namespace, worker_id, value);
}

/// Await task (blocking)
/// key = worker_id
/// value = [count:u32][task_types...] + options for block_ms, timeout_ms
/// block_ms: null = no blocking, 0 = block forever, >0 = block for N ms
pub fn workerAwait(
    client: *Client,
    namespace: []const u8,
    worker_id: []const u8,
    task_types: []const []const u8,
    block_ms: ?u32,
    timeout_ms: ?u32,
    _: ?u32, // max_tasks - not used
) !Response {
    var value_buf: [4096]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write task_types count
    try writer.writeInt(u32, @intCast(task_types.len), .little);

    // Write each task type
    for (task_types) |tt| {
        try writer.writeInt(u16, @intCast(tt.len), .little);
        try writer.writeAll(tt);
    }

    const value = fbs.getWritten();

    // Build options for block_ms and timeout_ms
    var options_buf: [32]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (block_ms) |b| {
        try builder.addU32(.block_ms, b);
        // Adjust socket read timeout for blocking requests
        if (b == 0) {
            client.setReadTimeoutSec(0); // infinite
        } else {
            client.setReadTimeoutSec(b / 1000 + 5);
        }
    }

    if (timeout_ms) |t| {
        try builder.addU32(.timeout_ms, t);
    }

    return client.sendRequestWithOptions(.worker_await, namespace, worker_id, value, builder.getOptions());
}

/// Touch task (extend lease)
/// key = worker_id
/// value = [action_name_len:u16][action_name][task_id_len:u16][task_id][extend_ms:u32]
pub fn workerTouch(
    client: *Client,
    namespace: []const u8,
    worker_id: []const u8,
    action_name: []const u8,
    task_id: []const u8,
    extend_ms: ?u32,
) !Response {
    var value_buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write action_name
    try writer.writeInt(u16, @intCast(action_name.len), .little);
    try writer.writeAll(action_name);

    // Write task_id
    try writer.writeInt(u16, @intCast(task_id.len), .little);
    try writer.writeAll(task_id);

    // Write extend_ms
    try writer.writeInt(u32, extend_ms orelse 30_000, .little);

    const value = fbs.getWritten();
    return client.sendRequest(.worker_touch, namespace, worker_id, value);
}

/// Complete task successfully
/// key = worker_id
/// value = [action_name_len:u16][action_name][task_id_len:u16][task_id][outcome_len:u16][outcome][result_len:u16][result]
pub fn workerComplete(
    client: *Client,
    namespace: []const u8,
    worker_id: []const u8,
    action_name: []const u8,
    task_id: []const u8,
    result: []const u8,
) !Response {
    var value_buf: [8192]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write action_name
    try writer.writeInt(u16, @intCast(action_name.len), .little);
    try writer.writeAll(action_name);

    // Write task_id
    try writer.writeInt(u16, @intCast(task_id.len), .little);
    try writer.writeAll(task_id);

    // Write outcome ("success")
    const outcome = "success";
    try writer.writeInt(u16, @intCast(outcome.len), .little);
    try writer.writeAll(outcome);

    // Write result
    try writer.writeInt(u16, @intCast(result.len), .little);
    try writer.writeAll(result);

    const value = fbs.getWritten();
    return client.sendRequest(.worker_complete, namespace, worker_id, value);
}

/// Fail task
/// key = worker_id  
/// value = [action_name_len:u16][action_name][task_id_len:u16][task_id][retry:u8][error_message...]
pub fn workerFail(
    client: *Client,
    namespace: []const u8,
    worker_id: []const u8,
    action_name: []const u8,
    task_id: []const u8,
    error_message: []const u8,
    retry: bool,
) !Response {
    var value_buf: [8192]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write action_name
    try writer.writeInt(u16, @intCast(action_name.len), .little);
    try writer.writeAll(action_name);

    // Write task_id
    try writer.writeInt(u16, @intCast(task_id.len), .little);
    try writer.writeAll(task_id);

    // Write retry flag
    try writer.writeByte(if (retry) 1 else 0);

    // Write error_message
    try writer.writeAll(error_message);

    const value = fbs.getWritten();
    return client.sendRequest(.worker_fail, namespace, worker_id, value);
}

/// List workers in namespace
/// key = empty
/// value = [limit:u32][cursor...]
pub fn workerList(
    client: *Client,
    namespace: []const u8,
    limit: ?u32,
    cursor: ?[]const u8,
) !Response {
    var value_buf: [256]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&value_buf);
    const writer = fbs.writer();

    // Write limit (default 100)
    try writer.writeInt(u32, limit orelse 100, .little);

    // Write cursor if provided (for shard walking)
    if (cursor) |c| {
        _ = writer.write(c) catch return error.BufferOverflow;
    }

    const value = fbs.getWritten();
    return client.sendRequest(.worker_list, namespace, "", value);
}
