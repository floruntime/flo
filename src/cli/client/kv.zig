//! KV Client Operations
//!
//! Key-Value operations for the Flo CLI client.
//! All functions take a *Client and namespace as parameters.

const base = @import("base.zig");
const Client = base.Client;
const Response = base.Response;
const proto = @import("../../node/protocol/proto.zig");

/// Execute a GET command
/// - wait_ms: Wait until exists (returns immediately if key exists, else waits)
/// - block_ms: Block for changes (waits for NEXT version even if key exists) - like stream/queue --block
/// - routing_key: Explicit routing override for shard co-location (same as {tag} in key)
pub fn get(client: *Client, namespace: []const u8, key: []const u8, wait_ms: ?u32, block_ms: ?u32, routing_key: ?[]const u8) !Response {
    if (wait_ms != null or block_ms != null or routing_key != null) {
        // Adjust socket read timeout for blocking requests:
        // - 0 means "wait forever" → disable socket timeout
        // - N > 0 means "wait N ms" → set timeout to N/1000 + 5s buffer
        const effective_ms: u32 = wait_ms orelse block_ms orelse 0;
        if (effective_ms == 0 and (wait_ms != null or block_ms != null)) {
            client.setReadTimeoutSec(0); // infinite
        } else if (effective_ms > 0) {
            const timeout_sec: u32 = effective_ms / 1000 + 5; // requested + 5s buffer
            client.setReadTimeoutSec(timeout_sec);
        }

        var options_buf: [64]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        if (wait_ms) |ms| {
            // wire .block_ms = wait-until-exists semantics
            builder.addU32(.block_ms, ms) catch return error.OptionsBufferTooSmall;
        }
        if (block_ms) |ms| {
            // wire .wait_ms = watch-for-changes semantics
            builder.addU32(.wait_ms, ms) catch return error.OptionsBufferTooSmall;
        }
        if (routing_key) |rk| {
            builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
        }
        return client.sendRequestWithOptions(.kv_get, namespace, key, "", builder.getOptions());
    }
    return client.sendRequest(.kv_get, namespace, key, "");
}

/// Options for SET command
pub const SetOptions = struct {
    ttl_seconds: ?u64 = null,
    if_not_exists: bool = false,
    if_exists: bool = false,
    cas_version: ?u64 = null,
    routing_key: ?[]const u8 = null,
};

/// Execute a SET command with options
pub fn set(client: *Client, namespace: []const u8, key: []const u8, value: []const u8, opts: SetOptions) !Response {
    var options_buf: [96]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (opts.ttl_seconds) |ttl| {
        builder.addU64(.ttl_seconds, ttl) catch return error.OptionsBufferTooSmall;
    }

    if (opts.if_not_exists) {
        builder.addFlag(.if_not_exists) catch return error.OptionsBufferTooSmall;
    }

    if (opts.if_exists) {
        builder.addFlag(.if_exists) catch return error.OptionsBufferTooSmall;
    }

    if (opts.cas_version) |ver| {
        builder.addU64(.cas_version, ver) catch return error.OptionsBufferTooSmall;
    }

    if (opts.routing_key) |rk| {
        builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
    }

    return client.sendRequestWithOptions(.kv_put, namespace, key, value, builder.getOptions());
}

/// Execute a DEL command
pub fn delete(client: *Client, namespace: []const u8, key: []const u8, routing_key: ?[]const u8) !Response {
    if (routing_key) |rk| {
        var options_buf: [64]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.kv_delete, namespace, key, "", builder.getOptions());
    }
    return client.sendRequest(.kv_delete, namespace, key, "");
}

/// Execute a SCAN command with full options including keys_only
pub fn scan(client: *Client, namespace: []const u8, prefix: []const u8, cursor: ?[]const u8, limit: ?u32, keys_only: bool) !Response {
    // Build options
    var options_buf: [64]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (limit) |l| {
        builder.addU32(.limit, l) catch return error.OptionsBufferTooSmall;
    }

    if (keys_only) {
        builder.addU8(.keys_only, 1) catch return error.OptionsBufferTooSmall;
    }

    const options = builder.getOptions();

    // kv_scan uses prefix in key field, cursor in value field
    return client.sendRequestWithOptions(.kv_scan, namespace, prefix, cursor orelse "", options);
}

/// Execute a HISTORY command (get version history for a key)
pub fn history(client: *Client, namespace: []const u8, key: []const u8, limit: ?u32) !Response {
    if (limit) |l| {
        var options_buf: [16]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        builder.addU32(.limit, l) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.kv_history, namespace, key, "", builder.getOptions());
    }
    return client.sendRequest(.kv_history, namespace, key, "");
}
