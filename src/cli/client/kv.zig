//! KV Client Operations
//!
//! Key-Value operations for the Flo CLI client.
//! All functions take a *Client and namespace as parameters.

const std = @import("std");
const base = @import("base.zig");
const Client = base.Client;
const Response = base.Response;
const proto = @import("../../protocol/proto.zig");

/// Execute a GET command
/// - wait_ms: Wait until exists (returns immediately if key exists, else waits)
/// - block_ms: Block for changes (waits for NEXT version even if key exists) - like stream/queue --block
/// - routing_key: Explicit routing override for shard co-location (same as {tag} in key)
pub fn get(client: *Client, namespace: []const u8, key: []const u8, wait_ms: ?u32, block_ms: ?u32, routing_key: ?[]const u8, txn_id: ?u64) !Response {
    if (wait_ms != null or block_ms != null or routing_key != null or txn_id != null) {
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

        var options_buf: [96]u8 = undefined;
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
        if (txn_id) |tid| {
            builder.addU64(.txn_id, tid) catch return error.OptionsBufferTooSmall;
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
    txn_id: ?u64 = null,
};

/// Execute a SET command with options
pub fn set(client: *Client, namespace: []const u8, key: []const u8, value: []const u8, opts: SetOptions) !Response {
    var options_buf: [128]u8 = undefined;
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

    if (opts.txn_id) |tid| {
        builder.addU64(.txn_id, tid) catch return error.OptionsBufferTooSmall;
    }

    return client.sendRequestWithOptions(.kv_put, namespace, key, value, builder.getOptions());
}

/// Execute a DEL command
pub fn delete(client: *Client, namespace: []const u8, key: []const u8, routing_key: ?[]const u8, txn_id: ?u64) !Response {
    if (routing_key != null or txn_id != null) {
        var options_buf: [96]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        if (routing_key) |rk| builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
        if (txn_id) |tid| builder.addU64(.txn_id, tid) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.kv_delete, namespace, key, "", builder.getOptions());
    }
    return client.sendRequest(.kv_delete, namespace, key, "");
}

/// Execute a SCAN command with full options including keys_only
pub fn scan(client: *Client, namespace: []const u8, prefix: []const u8, cursor: ?[]const u8, limit: ?u32, keys_only: bool) !Response {
    // Build TLV options (keys_only only — limit is in value now)
    var options_buf: [64]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (keys_only) {
        builder.addU8(.keys_only, 1) catch return error.OptionsBufferTooSmall;
    }

    const options = builder.getOptions();

    // Value: [limit:u32][cursor...]
    var value_buf: [4 + 1024]u8 = undefined;
    const cursor_bytes = cursor orelse "";
    if (4 + cursor_bytes.len > value_buf.len) return error.CursorTooLong;
    const lim: u32 = limit orelse 0; // 0 = server default
    std.mem.writeInt(u32, value_buf[0..4], lim, .little);
    if (cursor_bytes.len > 0) {
        @memcpy(value_buf[4 .. 4 + cursor_bytes.len], cursor_bytes);
    }

    return client.sendRequestWithOptions(.kv_scan, namespace, prefix, value_buf[0 .. 4 + cursor_bytes.len], options);
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

/// Execute a batch GET (MGET) command — multiple keys in one request.
/// Keys are packed in the value field: [count:u16]([key_len:u16][key])*
pub fn mget(client: *Client, namespace: []const u8, keys: []const []const u8) !Response {
    if (keys.len == 0) return error.EmptyBatch;
    if (keys.len > 256) return error.BatchTooLarge;

    // Pack keys into value field: [count:u16]([key_len:u16][key])*
    var value_buf: [64 * 1024]u8 = undefined; // 64KB for up to 256 keys
    var offset: usize = 0;

    // Write count
    std.mem.writeInt(u16, value_buf[offset..][0..2], @intCast(keys.len), .little);
    offset += 2;

    for (keys) |key| {
        if (offset + 2 + key.len > value_buf.len) return error.BatchTooLarge;
        std.mem.writeInt(u16, value_buf[offset..][0..2], @intCast(key.len), .little);
        offset += 2;
        @memcpy(value_buf[offset..][0..key.len], key);
        offset += key.len;
    }

    return client.sendRequest(.kv_mget, namespace, "", value_buf[0..offset]);
}

// ── Extended KV Operations (KV_ENHANCEMENTS phase 1) ─────────────────────

/// INCR — atomic counter increment. Wire format: value is 8-byte i64 LE delta.
/// Pass delta=0 to read-only increment-by-zero is rejected; default to 1 if 0.
/// Returns kv_value with 8-byte i64 LE counter value.
pub fn incr(client: *Client, namespace: []const u8, key: []const u8, delta: i64, routing_key: ?[]const u8, txn_id: ?u64) !Response {
    var val_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &val_buf, delta, .little);

    if (routing_key != null or txn_id != null) {
        var options_buf: [96]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        if (routing_key) |rk| builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
        if (txn_id) |tid| builder.addU64(.txn_id, tid) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.kv_incr, namespace, key, &val_buf, builder.getOptions());
    }
    return client.sendRequest(.kv_incr, namespace, key, &val_buf);
}

/// TOUCH — update an existing key's TTL. ttl_seconds=0 clears the TTL (same as PERSIST).
pub fn touch(client: *Client, namespace: []const u8, key: []const u8, ttl_seconds: u64, routing_key: ?[]const u8, txn_id: ?u64) !Response {
    var val_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &val_buf, ttl_seconds, .little);

    if (routing_key != null or txn_id != null) {
        var options_buf: [96]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        if (routing_key) |rk| builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
        if (txn_id) |tid| builder.addU64(.txn_id, tid) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.kv_touch, namespace, key, &val_buf, builder.getOptions());
    }
    return client.sendRequest(.kv_touch, namespace, key, &val_buf);
}

/// PERSIST — clear the TTL on an existing key. No value payload required.
pub fn persist(client: *Client, namespace: []const u8, key: []const u8, routing_key: ?[]const u8, txn_id: ?u64) !Response {
    if (routing_key != null or txn_id != null) {
        var options_buf: [96]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        if (routing_key) |rk| builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
        if (txn_id) |tid| builder.addU64(.txn_id, tid) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.kv_persist, namespace, key, "", builder.getOptions());
    }
    return client.sendRequest(.kv_persist, namespace, key, "");
}

/// EXISTS — check whether a key exists. Returns kv_value with a single byte: 0x01 if present, 0x00 if absent.
pub fn exists(client: *Client, namespace: []const u8, key: []const u8, routing_key: ?[]const u8, txn_id: ?u64) !Response {
    if (routing_key != null or txn_id != null) {
        var options_buf: [96]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        if (routing_key) |rk| builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
        if (txn_id) |tid| builder.addU64(.txn_id, tid) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.kv_exists, namespace, key, "", builder.getOptions());
    }
    return client.sendRequest(.kv_exists, namespace, key, "");
}

/// JSON.GET — extract a JSONPath subtree from a JSON-encoded value.
/// Path syntax: $, $.field, $.a.b, $.arr[0]. Defaults to "$" if empty.
pub fn jsonGet(client: *Client, namespace: []const u8, key: []const u8, path: []const u8, routing_key: ?[]const u8) !Response {
    if (routing_key) |rk| {
        var options_buf: [64]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.kv_json_get, namespace, key, path, builder.getOptions());
    }
    return client.sendRequest(.kv_json_get, namespace, key, path);
}

/// JSON.SET — set a JSON value at `path` (read-modify-write).
/// Wire format: value = [path_len:u16][path][json].
pub fn jsonSet(client: *Client, namespace: []const u8, key: []const u8, path: []const u8, json_value: []const u8, routing_key: ?[]const u8) !Response {
    if (path.len > 0xFFFF) return error.PathTooLong;
    const total = 2 + path.len + json_value.len;
    const buf = try client.allocator.alloc(u8, total);
    defer client.allocator.free(buf);

    std.mem.writeInt(u16, buf[0..2], @intCast(path.len), .little);
    @memcpy(buf[2 .. 2 + path.len], path);
    @memcpy(buf[2 + path.len ..], json_value);

    if (routing_key) |rk| {
        var options_buf: [64]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.kv_json_set, namespace, key, buf, builder.getOptions());
    }
    return client.sendRequest(.kv_json_set, namespace, key, buf);
}

/// JSON.DEL — remove the value at `path`. Path "$" deletes the entire key.
pub fn jsonDel(client: *Client, namespace: []const u8, key: []const u8, path: []const u8, routing_key: ?[]const u8) !Response {
    if (routing_key) |rk| {
        var options_buf: [64]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        builder.addString(.routing_key, rk) catch return error.OptionsBufferTooSmall;
        return client.sendRequestWithOptions(.kv_json_del, namespace, key, path, builder.getOptions());
    }
    return client.sendRequest(.kv_json_del, namespace, key, path);
}

// ── Per-Shard Transactions ───────────────────────────────────────────────

/// BEGIN — open a per-shard transaction pinned to `routing_key`'s partition.
/// On success the response is a `kv_txn_response` carrying the new txn_id.
/// Use `Response.getTxnId()` to extract it.
pub fn beginTxn(client: *Client, namespace: []const u8, routing_key: []const u8) !Response {
    var options_buf: [96]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);
    if (routing_key.len > 0) {
        builder.addString(.routing_key, routing_key) catch return error.OptionsBufferTooSmall;
    }
    return client.sendRequestWithOptions(.kv_begin_txn, namespace, routing_key, "", builder.getOptions());
}

/// COMMIT — atomically apply all buffered ops in the transaction.
/// `routing_key` must be the same one used at BEGIN so the request reaches
/// the txn's pinned shard.
pub fn commitTxn(client: *Client, namespace: []const u8, routing_key: []const u8, txn_id: u64) !Response {
    var options_buf: [96]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);
    if (routing_key.len > 0) builder.addString(.routing_key, routing_key) catch return error.OptionsBufferTooSmall;
    builder.addU64(.txn_id, txn_id) catch return error.OptionsBufferTooSmall;
    return client.sendRequestWithOptions(.kv_commit_txn, namespace, routing_key, "", builder.getOptions());
}

/// ROLLBACK — discard the in-memory write set without committing.
pub fn rollbackTxn(client: *Client, namespace: []const u8, routing_key: []const u8, txn_id: u64) !Response {
    var options_buf: [96]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);
    if (routing_key.len > 0) builder.addString(.routing_key, routing_key) catch return error.OptionsBufferTooSmall;
    builder.addU64(.txn_id, txn_id) catch return error.OptionsBufferTooSmall;
    return client.sendRequestWithOptions(.kv_rollback_txn, namespace, routing_key, "", builder.getOptions());
}
