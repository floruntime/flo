//! KV Client Operations
//!
//! Every KV op is a table op: its body is the op's request schema, and its
//! answer, when ok, is the op's response schema.

const std = @import("std");
const base = @import("base.zig");
const Client = base.Client;
const Response = base.Response;
const kv_ops = @import("../../protocol/kv_ops.zig");

pub const get_op = kv_ops.op("kv.get");
pub const set_op = kv_ops.op("kv.set");

/// Send KV op `name` with `body`. A get that waits is read for as long plus
/// 5 s.
pub fn call(client: *Client, comptime name: []const u8, namespace: []const u8, key: []const u8, body: kv_ops.op(name).request.Value) !Response {
    if (comptime std.mem.eql(u8, name, "kv.get")) {
        if (body.wait_ms > 0) client.setReadTimeoutSec(@intCast(body.wait_ms / 1000 + 5));
    }
    return client.call(kv_ops.op(name), namespace, key, body);
}

/// Read a key.
pub fn get(client: *Client, namespace: []const u8, key: []const u8, body: get_op.request.Value) !Response {
    return call(client, "kv.get", namespace, key, body);
}

/// Write a key.
pub fn set(client: *Client, namespace: []const u8, key: []const u8, body: set_op.request.Value) !Response {
    return call(client, "kv.set", namespace, key, body);
}
