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

/// List processing jobs
/// Wire format:
///   - namespace: req.namespace
///   - key: (unused)
///   - value: cursor bytes (empty on first call)
pub fn list(
    client: *Client,
    namespace: []const u8,
    cursor: ?[]const u8,
) !Response {
    return client.sendRequest(.processing_list, namespace, "", cursor orelse "");
}

/// Trigger a savepoint for a processing job
/// Wire format:
///   - namespace: req.namespace
///   - key: job_id
///   - value: (unused)
pub fn savepoint(
    client: *Client,
    namespace: []const u8,
    job_id: []const u8,
) !Response {
    return client.sendRequest(.processing_savepoint, namespace, job_id, "");
}

/// Restore a processing job from a savepoint
/// Wire format:
///   - namespace: req.namespace
///   - key: job_id
///   - value: savepoint_id
pub fn restore(
    client: *Client,
    namespace: []const u8,
    job_id: []const u8,
    savepoint_id: []const u8,
) !Response {
    return client.sendRequest(.processing_restore, namespace, job_id, savepoint_id);
}

/// Rescale a processing job's parallelism
/// Wire format:
///   - namespace: req.namespace
///   - key: job_id
///   - value: [parallelism:u32]
pub fn rescale(
    client: *Client,
    namespace: []const u8,
    job_id: []const u8,
    parallelism: u32,
) !Response {
    var value_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &value_buf, parallelism, .little);
    return client.sendRequest(.processing_rescale, namespace, job_id, &value_buf);
}
