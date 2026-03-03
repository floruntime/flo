//! KV Handler — registers KV opcodes with Dispatcher and handles KV operations.
//!
//! Read operations (get, scan, history) query the KV projection directly.
//! Write operations (put, delete) go through the KV projection directly for now;
//! they will be rewired through Raft propose when the full pipeline is connected.
//!
//! ## Handler Registration
//!
//! ```zig
//! var handler = KVHandler.init(allocator, &partition.kv);
//! handler.register(&dispatcher);
//! ```
//!
//! ## Dispatch Flow
//!
//! Acceptor → Shard → Dispatcher → KVHandler.dispatch{Get,Put,...}
//!   → handleCommand(kv, op, req) → CommandResult
//!   → serialize → Connection.queueWrite
//!
//! ## Reserved Keys
//!
//! Keys prefixed with `_action:`, `_worker:`, `_sys:`, `_internal:`, `_flo:`
//! are system-owned and blocked from user operations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const kv_mod = @import("../projection/kv.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");

const CommandResult = result_mod.CommandResult;
const KVProjection = kv_mod.KVProjection;
const ScanEntry = kv_mod.ScanEntry;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const OptionsBuilder = proto.OptionsBuilder;
const KVWAL = @import("../storage/kv_wal.zig").KVWAL;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

/// Reserved key prefixes — operations on these are blocked for user requests.
const RESERVED_PREFIXES = [_][]const u8{
    "_action:",
    "_worker:",
    "_sys:",
    "_internal:",
    "_flo:",
};

/// Maximum number of scan results in a single response.
const MAX_SCAN_LIMIT: u32 = 1000;

/// Default scan limit when no limit option is provided.
const DEFAULT_SCAN_LIMIT: u32 = 100;

// ═══════════════════════════════════════════════════════════════════════════════
// KVHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const KVHandler = struct {
    kv: *KVProjection,
    allocator: Allocator,

    /// Monotonic LSN counter — stand-in for Raft index until full pipeline.
    next_lsn: u64,

    pub fn init(allocator: Allocator, kv: *KVProjection) KVHandler {
        return .{
            .kv = kv,
            .allocator = allocator,
            .next_lsn = 1,
        };
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    /// Register KV opcode handlers with the Dispatcher.
    /// Uses pre-route hooks for routing key extraction.
    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.kv_get, dispatchGet, preRouteByKey);
        dispatcher.registerWithRoute(.kv_put, dispatchPut, preRouteByKey);
        dispatcher.registerWithRoute(.kv_delete, dispatchDelete, preRouteByKey);
        dispatcher.registerWithRoute(.kv_scan, dispatchScan, preRouteScan);
        dispatcher.register(.kv_history, dispatchHistory);
    }

    // ── Pre-Route Hooks ─────────────────────────────────────────────────

    /// Route by key hash — single-partition operations.
    fn preRouteByKey(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        // Check for explicit routing key option
        if (req.findOption(.routing_key)) |opt| {
            return std.hash.Wyhash.hash(0, opt.asString());
        }
        return std.hash.Wyhash.hash(0, req.key);
    }

    /// Scan may be single-partition (prefix) or multi-shard (full scan).
    fn preRouteScan(req: Request) ?u64 {
        if (req.key.len > 0) {
            // Prefix scan — route to the prefix's partition
            return std.hash.Wyhash.hash(0, req.key);
        }
        // Full scan — requires ShardWalker (return null)
        return null;
    }

    // ── Dispatch Wrappers ───────────────────────────────────────────────
    // These bridge from Dispatcher's (shard, conn, req) signature.
    // When Shard owns Partitions, these will extract the correct
    // Partition's KVProjection based on routing.

    fn dispatchGet(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.kv_handler.handleCommand(req);
        defer shard.kv_handler.freeResult(cmd_result);
        sendKVResponse(shard, conn, req.header.request_id, cmd_result);
    }

    fn dispatchPut(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.kv_handler.handleCommand(req);
        defer shard.kv_handler.freeResult(cmd_result);

        // Persist successful puts to WAL
        switch (cmd_result) {
            .kv_put_ok => {
                if (shard.kv_wal) |wal| {
                    const ttl = req.getTtlSeconds() orelse 0;
                    const now = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
                    const expiry: u64 = if (ttl > 0) now + ttl * 1_000_000_000 else 0;
                    wal.appendPut(req.key, req.value, expiry) catch {};
                    wal.sync();
                }
            },
            else => {},
        }

        sendKVResponse(shard, conn, req.header.request_id, cmd_result);
    }

    fn dispatchDelete(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.kv_handler.handleCommand(req);
        defer shard.kv_handler.freeResult(cmd_result);

        // Persist successful deletes to WAL
        switch (cmd_result) {
            .ok => {
                if (shard.kv_wal) |wal| {
                    wal.appendDelete(req.key) catch {};
                    wal.sync();
                }
            },
            else => {},
        }

        sendKVResponse(shard, conn, req.header.request_id, cmd_result);
    }

    fn dispatchScan(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.kv_handler.handleCommand(req);
        defer shard.kv_handler.freeResult(cmd_result);
        sendKVResponse(shard, conn, req.header.request_id, cmd_result);
    }

    fn dispatchHistory(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const cmd_result = shard.kv_handler.handleCommand(req);
        defer shard.kv_handler.freeResult(cmd_result);
        sendKVResponse(shard, conn, req.header.request_id, cmd_result);
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    /// Dispatch a KV command to the appropriate handler.
    /// Returns a CommandResult that the caller serializes and sends.
    pub fn handleCommand(self: *KVHandler, req: Request) CommandResult {
        const op: OpCode = @enumFromInt(req.header.op_code);
        return switch (op) {
            .kv_get => self.handleGet(req),
            .kv_put => self.handlePut(req),
            .kv_delete => self.handleDelete(req),
            .kv_scan => self.handleScan(req),
            .kv_history => self.handleHistory(req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown KV opcode" } },
        };
    }

    // ── GET ─────────────────────────────────────────────────────────────

    fn handleGet(self: *KVHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }

        if (isReservedKey(req.key)) {
            return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };
        }

        const entry = self.kv.get(req.key) orelse return .kv_not_found;
        return .{ .kv_value = .{ .value = entry.value, .version = entry.lsn } };
    }

    // ── PUT ─────────────────────────────────────────────────────────────

    fn handlePut(self: *KVHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }

        if (isReservedKey(req.key)) {
            return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };
        }

        // Check compare-and-swap (CAS) — version must match current
        if (req.getCasVersion()) |expected_version| {
            const current = self.kv.get(req.key);
            if (current) |entry| {
                if (entry.lsn != expected_version) {
                    return .{ .kv_cas_failed = .{ .current_version = entry.lsn } };
                }
            } else {
                // CAS on non-existent key: version 0 means "must not exist"
                if (expected_version != 0) {
                    return .{ .kv_cas_failed = .{ .current_version = 0 } };
                }
            }
        }

        // Check if_not_exists condition (NX)
        if (req.getIfNotExists()) {
            if (self.kv.get(req.key) != null) {
                return .kv_condition_not_met;
            }
        }

        // Check if_exists condition (XX)
        if (req.getIfExists()) {
            if (self.kv.get(req.key) == null) {
                return .kv_condition_not_met;
            }
        }

        // TODO: Replace direct put with Raft propose for write path
        const lsn = self.nextLsn();
        const timestamp = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

        // Compute TTL expiry (nanoseconds from epoch)
        const expiry_ns: u64 = if (req.getTtlSeconds()) |ttl_secs| blk: {
            if (ttl_secs == 0) break :blk 0; // 0 means no expiry
            break :blk timestamp + ttl_secs * 1_000_000_000;
        } else 0;

        self.kv.put(req.key, req.value, lsn, 0, timestamp, expiry_ns) catch {
            return .{ .err = .{ .code = .internal_error, .message = "put failed" } };
        };

        return .{ .kv_put_ok = .{ .version = lsn } };
    }

    // ── DELETE ──────────────────────────────────────────────────────────

    fn handleDelete(self: *KVHandler, req: Request) CommandResult {
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }

        if (isReservedKey(req.key)) {
            return .{ .err = .{ .code = .unauthorized, .message = "access to reserved key denied" } };
        }

        // TODO: Replace direct delete with Raft propose for write path
        const lsn = self.nextLsn();
        const timestamp = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

        self.kv.delete(req.key, lsn, 0, timestamp) catch {
            return .{ .err = .{ .code = .internal_error, .message = "delete failed" } };
        };

        return .ok;
    }

    // ── SCAN ────────────────────────────────────────────────────────────

    fn handleScan(self: *KVHandler, req: Request) CommandResult {
        const limit = req.getLimit() orelse DEFAULT_SCAN_LIMIT;
        const capped_limit = @min(limit, MAX_SCAN_LIMIT);

        // Allocate scan buffer on stack
        var scan_buf: [MAX_SCAN_LIMIT]ScanEntry = undefined;
        const out = scan_buf[0..capped_limit];

        // Prefix scan if key is provided, otherwise full scan
        const found_count = if (req.key.len > 0)
            self.kv.scanPrefix(req.key, out)
        else
            self.kv.scan(out);

        // Filter out reserved keys from results
        var filtered_count: usize = 0;
        for (out[0..found_count]) |entry| {
            if (!isReservedKey(entry.key)) {
                scan_buf[filtered_count] = entry;
                filtered_count += 1;
            }
        }

        const keys_only = req.getKeysOnly();

        // Serialize scan results
        const data = serializeScanResults(self.allocator, scan_buf[0..filtered_count], keys_only) catch {
            return .{ .err = .{ .code = .internal_error, .message = "scan serialization failed" } };
        };

        return .{ .kv_scan_result = .{ .data = data } };
    }

    // ── HISTORY ─────────────────────────────────────────────────────────

    fn handleHistory(self: *KVHandler, req: Request) CommandResult {
        _ = self;
        if (req.key.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "key is required" } };
        }

        // Version history not yet implemented in KVProjection MVCC
        return .{ .err = .{ .code = .invalid_request, .message = "history not yet implemented" } };
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn nextLsn(self: *KVHandler) u64 {
        const lsn = self.next_lsn;
        self.next_lsn += 1;
        return lsn;
    }

    /// Free any heap-allocated data in a CommandResult returned by this handler.
    /// Currently only kv_scan_result allocates.
    pub fn freeResult(self: *KVHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .kv_scan_result => |r| self.allocator.free(r.data),
            else => {},
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Response Serialization — CommandResult → Wire Response
// ═══════════════════════════════════════════════════════════════════════════════

/// Maximum response buffer: 24-byte header + 8-byte prefix + 256KB payload
const MAX_RESPONSE_BUF = @sizeOf(proto.ResponseHeader) + 8 + (256 * 1024);

/// Convert a CommandResult to a wire response and queue it on the connection.
/// Handles all KV result variants: kv_value (with version prefix), kv_put_ok,
/// kv_not_found, kv_cas_failed, kv_condition_not_met, kv_scan_result, ok, err.
fn sendKVResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .kv_value => |v| {
            // CLI expects: [version:u64 LE][value bytes]
            var resp = proto.Response.init(request_id, .ok, v.value);
            resp.prefix = v.version;
            var buf: [MAX_RESPONSE_BUF]u8 = undefined;
            const serialized = resp.serialize(&buf) catch return;
            _ = conn.queueWrite(serialized);
        },
        .kv_not_found => {
            var resp = proto.Response.initError(request_id, .not_found);
            var buf: [128]u8 = undefined;
            const serialized = resp.serialize(&buf) catch return;
            _ = conn.queueWrite(serialized);
        },
        .kv_put_ok => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .ok => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .kv_cas_failed => {
            var resp = proto.Response.initError(request_id, .conflict);
            var buf: [128]u8 = undefined;
            const serialized = resp.serialize(&buf) catch return;
            _ = conn.queueWrite(serialized);
        },
        .kv_condition_not_met => {
            var resp = proto.Response.initError(request_id, .conflict);
            var buf: [128]u8 = undefined;
            const serialized = resp.serialize(&buf) catch return;
            _ = conn.queueWrite(serialized);
        },
        .kv_scan_result => |scan| {
            shard.sendOkResponse(conn, request_id, scan.data);
        },
        .kv_history_result => |hist| {
            shard.sendOkResponse(conn, request_id, hist.data);
        },
        .err => |e| {
            const status = errorCodeToStatus(e.code);
            shard.sendErrorResponse(conn, request_id, status, e.message);
        },
        else => {
            shard.sendErrorResponse(conn, request_id, .internal_error, "unexpected result type");
        },
    }
}

/// Map CommandResult.ErrorCode to wire StatusCode.
fn errorCodeToStatus(code: CommandResult.ErrorCode) proto.StatusCode {
    return switch (code) {
        .invalid_request => .bad_request,
        .unauthorized => .unauthorized,
        .not_found => .not_found,
        .already_exists => .conflict,
        .timeout => .internal_error,
        .internal_error => .internal_error,
        .unavailable => .internal_error,
        .kv_key_too_large => .bad_request,
        .kv_value_too_large => .bad_request,
        .kv_namespace_not_found => .not_found,
        .kv_txn_conflict => .conflict,
        .conflict => .conflict,
        else => .internal_error,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Utilities
// ═══════════════════════════════════════════════════════════════════════════════

/// Check if a key is reserved (system-owned).
fn isReservedKey(key: []const u8) bool {
    for (RESERVED_PREFIXES) |prefix| {
        if (key.len >= prefix.len and std.mem.eql(u8, key[0..prefix.len], prefix)) {
            return true;
        }
    }
    return false;
}

/// Serialize scan results to binary format.
/// Wire format: [count:u32] ([key_len:u16][key][value_len:u32][value])* [has_more:u8]
/// When keys_only=true, value_len is 0 and value is empty (field is still present).
fn serializeScanResults(allocator: Allocator, entries: []const ScanEntry, keys_only: bool) ![]u8 {
    // Calculate total size
    var total: usize = 4; // count header
    for (entries) |entry| {
        total += 2 + entry.key.len; // key_len + key
        // Always include value_len field; when keys_only, value_len=0
        if (keys_only) {
            total += 4; // value_len only (0)
        } else {
            total += 4 + entry.value.len; // value_len + value
        }
    }
    total += 1; // has_more flag

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var offset: usize = 0;

    // Count
    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(entries.len), .little);
    offset += 4;

    for (entries) |entry| {
        // Key
        std.mem.writeInt(u16, buf[offset..][0..2], @intCast(entry.key.len), .little);
        offset += 2;
        @memcpy(buf[offset..][0..entry.key.len], entry.key);
        offset += entry.key.len;

        // Value — always present; empty when keys_only
        if (keys_only) {
            std.mem.writeInt(u32, buf[offset..][0..4], 0, .little);
            offset += 4;
        } else {
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(entry.value.len), .little);
            offset += 4;
            @memcpy(buf[offset..][0..entry.value.len], entry.value);
            offset += entry.value.len;
        }
    }

    // has_more — always false for now (no cursor pagination yet)
    buf[offset] = 0;

    return buf;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

/// Helper to build a test request for a given opcode.
fn makeRequest(op: OpCode, key: []const u8, value: []const u8, options: []const u8) Request {
    return .{
        .header = .{
            .magic = proto.MAGIC,
            .payload_length = 0,
            .request_id = 1,
            .crc32 = 0,
            .version = proto.VERSION,
            .op_code = @intFromEnum(op),
            .flags = 0,
            .reserved = 0,
        },
        .namespace = "default",
        .key = key,
        .value = value,
        .options = options,
    };
}

test "kv handler: get existing key" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    try kv.put("hello", "world", 1, 0, 1000, 0);

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_get, "hello", "", ""));

    switch (result) {
        .kv_value => |v| {
            try testing.expectEqualStrings("world", v.value);
            try testing.expectEqual(@as(u64, 1), v.version);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: get non-existent key" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_get, "nope", "", ""));

    switch (result) {
        .kv_not_found => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: get empty key returns error" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_get, "", "", ""));

    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: get reserved key blocked" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    // Manually put a reserved key (bypass handler)
    try kv.put("_sys:config", "secret", 1, 0, 1000, 0);

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_get, "_sys:config", "", ""));

    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.unauthorized, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put and get" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Put
    const put_result = handler.handleCommand(makeRequest(.kv_put, "mykey", "myval", ""));
    switch (put_result) {
        .kv_put_ok => |p| try testing.expectEqual(@as(u64, 1), p.version),
        else => return error.TestUnexpectedResult,
    }

    // Get it back
    const get_result = handler.handleCommand(makeRequest(.kv_get, "mykey", "", ""));
    switch (get_result) {
        .kv_value => |v| try testing.expectEqualStrings("myval", v.value),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put with CAS success" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Initial put (version = 1)
    _ = handler.handleCommand(makeRequest(.kv_put, "k", "v1", ""));

    // CAS put with version 1 → should succeed
    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addU64(.cas_version, 1);
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.kv_put, "k", "v2", opts));
    switch (result) {
        .kv_put_ok => {},
        else => return error.TestUnexpectedResult,
    }

    // Verify updated
    const get_res = handler.handleCommand(makeRequest(.kv_get, "k", "", ""));
    switch (get_res) {
        .kv_value => |v| try testing.expectEqualStrings("v2", v.value),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put with CAS failure" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Initial put (version = 1)
    _ = handler.handleCommand(makeRequest(.kv_put, "k", "v1", ""));

    // CAS put with wrong version 42 → should fail
    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addU64(.cas_version, 42);
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.kv_put, "k", "v2", opts));
    switch (result) {
        .kv_cas_failed => |c| try testing.expectEqual(@as(u64, 1), c.current_version),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put if_not_exists success" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addFlag(.if_not_exists);
    const opts = builder.getOptions();

    // Key doesn't exist → should succeed
    const result = handler.handleCommand(makeRequest(.kv_put, "new_key", "val", opts));
    switch (result) {
        .kv_put_ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put if_not_exists failure" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Pre-populate
    _ = handler.handleCommand(makeRequest(.kv_put, "existing", "val", ""));

    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addFlag(.if_not_exists);
    const opts = builder.getOptions();

    // Key exists → should fail
    const result = handler.handleCommand(makeRequest(.kv_put, "existing", "new_val", opts));
    switch (result) {
        .kv_condition_not_met => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put if_exists success" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Pre-populate
    _ = handler.handleCommand(makeRequest(.kv_put, "existing", "old", ""));

    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addFlag(.if_exists);
    const opts = builder.getOptions();

    // Key exists → should succeed
    const result = handler.handleCommand(makeRequest(.kv_put, "existing", "new", opts));
    switch (result) {
        .kv_put_ok => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: put if_exists failure" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addFlag(.if_exists);
    const opts = builder.getOptions();

    // Key doesn't exist → should fail
    const result = handler.handleCommand(makeRequest(.kv_put, "missing", "val", opts));
    switch (result) {
        .kv_condition_not_met => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: delete" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Put then delete
    _ = handler.handleCommand(makeRequest(.kv_put, "k", "v", ""));
    const del_result = handler.handleCommand(makeRequest(.kv_delete, "k", "", ""));
    switch (del_result) {
        .ok => {},
        else => return error.TestUnexpectedResult,
    }

    // Get should return not_found
    const get_result = handler.handleCommand(makeRequest(.kv_get, "k", "", ""));
    switch (get_result) {
        .kv_not_found => {},
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: delete reserved key blocked" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_delete, "_flo:metadata", "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.unauthorized, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: scan empty" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_scan, "", "", ""));

    switch (result) {
        .kv_scan_result => |r| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: scan with results" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Insert some keys
    _ = handler.handleCommand(makeRequest(.kv_put, "a", "1", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "b", "2", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "c", "3", ""));

    const result = handler.handleCommand(makeRequest(.kv_scan, "", "", ""));
    switch (result) {
        .kv_scan_result => |r| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 3), count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: scan with limit" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);

    // Insert 5 keys
    _ = handler.handleCommand(makeRequest(.kv_put, "a", "1", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "b", "2", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "c", "3", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "d", "4", ""));
    _ = handler.handleCommand(makeRequest(.kv_put, "e", "5", ""));

    // Scan with limit 2
    var opts_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&opts_buf);
    try builder.addU32(.limit, 2);
    const opts = builder.getOptions();

    const result = handler.handleCommand(makeRequest(.kv_scan, "", "", opts));
    switch (result) {
        .kv_scan_result => |r| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expect(count <= 2);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: scan filters reserved keys" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    // Insert user key and reserved key directly into projection
    try kv.put("user_key", "val", 1, 0, 1000, 0);
    try kv.put("_sys:hidden", "secret", 2, 0, 2000, 0);

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_scan, "", "", ""));

    switch (result) {
        .kv_scan_result => |r| {
            defer handler.freeResult(result);
            const count = std.mem.readInt(u32, r.data[0..4], .little);
            // Only user_key should appear
            try testing.expectEqual(@as(u32, 1), count);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    KVHandler.register(&dispatcher);

    // Verify handlers were registered for KV opcodes
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_get)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_put)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_delete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_scan)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.kv_history)] != null);

    // Verify pre-route hooks
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_get)] != null);
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_put)] != null);
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_scan)] != null);
    try testing.expect(dispatcher.pre_route[@intFromEnum(OpCode.kv_history)] == null); // no routing for history

    // 5 handlers registered
    try testing.expectEqual(@as(u16, 5), dispatcher.handler_count);
}

test "kv handler: pre-route by key" {
    const req1 = makeRequest(.kv_get, "key1", "", "");
    const req2 = makeRequest(.kv_get, "key1", "", "");
    const req3 = makeRequest(.kv_get, "key2", "", "");

    // Same key → same hash
    const h1 = KVHandler.preRouteByKey(req1);
    const h2 = KVHandler.preRouteByKey(req2);
    try testing.expectEqual(h1, h2);

    // Different key → different hash (with overwhelming probability)
    const h3 = KVHandler.preRouteByKey(req3);
    try testing.expect(h1 != h3);

    // Empty key → hash 0
    const req_empty = makeRequest(.kv_get, "", "", "");
    try testing.expectEqual(@as(?u64, 0), KVHandler.preRouteByKey(req_empty));
}

test "kv handler: pre-route scan" {
    // Prefix scan → non-null hash
    const req_prefix = makeRequest(.kv_scan, "prefix", "", "");
    try testing.expect(KVHandler.preRouteScan(req_prefix) != null);

    // Full scan → null (multi-shard)
    const req_full = makeRequest(.kv_scan, "", "", "");
    try testing.expect(KVHandler.preRouteScan(req_full) == null);
}

test "kv handler: history not implemented" {
    const allocator = testing.allocator;
    var kv = KVProjection.init(allocator, 0);
    defer kv.deinit();

    var handler = KVHandler.init(allocator, &kv);
    const result = handler.handleCommand(makeRequest(.kv_history, "k", "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "kv handler: reserved key prefixes" {
    try testing.expect(isReservedKey("_action:compute"));
    try testing.expect(isReservedKey("_worker:job1"));
    try testing.expect(isReservedKey("_sys:config"));
    try testing.expect(isReservedKey("_internal:state"));
    try testing.expect(isReservedKey("_flo:metadata"));
    try testing.expect(!isReservedKey("normal_key"));
    try testing.expect(!isReservedKey("_other:prefix")); // not a reserved prefix
    try testing.expect(!isReservedKey("")); // empty key
}
