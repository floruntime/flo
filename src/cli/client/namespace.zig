//! Namespace Client Operations
//!
//! The namespace ops, encoded by the op table. The namespace an op acts on
//! is its key; the envelope's namespace is unused.

const base = @import("base.zig");
const namespace_ops = @import("../../protocol/namespace_ops.zig");
const Client = base.Client;
const Response = base.Response;

pub fn create(client: *Client, name: []const u8) !Response {
    return client.call(namespace_ops.op("namespace.create"), "", name, .{});
}

pub fn delete(client: *Client, name: []const u8) !Response {
    return client.call(namespace_ops.op("namespace.delete"), "", name, .{});
}

/// One page of names after `cursor` (empty to start).
pub fn list(client: *Client, cursor: []const u8) !Response {
    return client.call(namespace_ops.op("namespace.list"), "", "", .{ .limit = namespace_ops.MAX_LIST, .cursor = cursor });
}

/// Set `name`'s run-history quota: the default, none, or `bytes` over every
/// shard (with the bytes mode only).
pub fn alter(client: *Client, name: []const u8, mode: namespace_ops.QuotaMode, bytes: ?u64) !Response {
    return client.call(namespace_ops.op("namespace.alter"), "", name, .{ .history_quota_mode = mode, .history_quota_bytes = bytes });
}

pub fn info(client: *Client, name: []const u8) !Response {
    return client.call(namespace_ops.op("namespace.info"), "", name, .{});
}
