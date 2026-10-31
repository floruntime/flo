//! Processing Client Operations
//!
//! Stream processing operations for the Flo CLI client.
//! Uses the Request format: namespace, key, value
//!
//! Wire format conventions (from conversion.zig):
//!   - namespace: always req.namespace
//!   - key: job_id (for most commands)
//!   - value: command-specific data

const std = @import("std");
const base = @import("base.zig");
const Client = base.Client;
const Response = base.Response;
const processing_ops = @import("../../protocol/processing_ops.zig");

// =============================================================================
// Processing Operations
// =============================================================================

/// Submit a processing job definition (YAML)
/// Wire format:
///   - namespace: req.namespace
///   - key: (unused)
///   - value: definition_yaml
pub fn submit(
    client: *Client,
    namespace: []const u8,
    definition_yaml: []const u8,
) !Response {
    return client.sendRequest(.processing_submit, namespace, "", definition_yaml);
}

/// Gracefully stop a processing job
/// Wire format:
///   - namespace: req.namespace
///   - key: job_id
///   - value: (unused)
pub fn stop(
    client: *Client,
    namespace: []const u8,
    job_id: []const u8,
) !Response {
    return client.sendRequest(.processing_stop, namespace, job_id, "");
}

/// Force cancel a processing job
/// Wire format:
///   - namespace: req.namespace
///   - key: job_id
///   - value: (unused)
pub fn cancel(
    client: *Client,
    namespace: []const u8,
    job_id: []const u8,
) !Response {
    return client.sendRequest(.processing_cancel, namespace, job_id, "");
}

/// Remove a stopped, cancelled or failed job, freeing its slot.
pub fn remove(
    client: *Client,
    namespace: []const u8,
    job_id: []const u8,
) !Response {
    return client.call(processing_ops.op("processing.remove"), namespace, job_id, .{});
}

/// Get processing job status
/// Wire format:
///   - namespace: req.namespace
///   - key: job_id
///   - value: (unused)
pub fn status(
    client: *Client,
    namespace: []const u8,
    job_id: []const u8,
) !Response {
    return client.sendRequest(.processing_status, namespace, job_id, "");
}

/// List processing jobs; value [limit:u32][cursor] like every list op.
pub fn list(
    client: *Client,
    namespace: []const u8,
    limit: ?u32,
    cursor: ?[]const u8,
) !Response {
    var value_buf: [base.WALK_VALUE_MAX]u8 = undefined;
    return client.sendRequest(.processing_list, namespace, "", try base.walkValue(&value_buf, limit, cursor));
}
