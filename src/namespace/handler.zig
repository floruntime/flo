//! Namespace Handler — registers namespace management opcodes with Dispatcher.
//!
//! Namespace operations are controller-only commands that route to Shard 0.
//! They manage the namespace registry (create, delete, list, info).
//!
//! ## Opcode Range
//!
//!   Commands:   0xB0–0xB3  (create, delete, list, info)
//!   Responses:  0xB4–0xB7
//!
//! ## Namespace Semantics
//!
//! - The namespace name is passed in `req.key` (not `req.namespace`).
//! - All mutations go through Controller Raft on Shard 0 (when wired).
//! - No pre-route hooks — all namespace commands route to controller.
//! - Reserved namespaces (`_sys`, `_internal`, `_flo`) cannot be created/deleted.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");

const CommandResult = result_mod.CommandResult;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

// ═══════════════════════════════════════════════════════════════════════════════
// NamespaceHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const NamespaceHandler = struct {
    allocator: Allocator,

    /// In-memory namespace registry. Keys are owned copies of namespace names.
    /// Will be replaced by Controller Raft storage when wired.
    namespaces: std.StringHashMap(NamespaceMeta),

    const MAX_NAMESPACE_LEN: usize = 128;
    const MAX_NAMESPACES: usize = 1024;

    const reserved_prefixes = [_][]const u8{
        "_sys",
        "_internal",
        "_flo",
    };

    pub const NamespaceMeta = struct {
        created_at_ns: u64,
    };

    pub fn init(allocator: Allocator) NamespaceHandler {
        return .{
            .allocator = allocator,
            .namespaces = std.StringHashMap(NamespaceMeta).init(allocator),
        };
    }

    pub fn deinit(self: *NamespaceHandler) void {
        var it = self.namespaces.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.namespaces.deinit();
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        // No pre-route hooks — namespace commands route to Controller (Shard 0).
        dispatcher.register(.namespace_create, dispatchNamespace);
        dispatcher.register(.namespace_delete, dispatchNamespace);
        dispatcher.register(.namespace_list, dispatchNamespace);
        dispatcher.register(.namespace_info, dispatchNamespace);
    }

    fn dispatchNamespace(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.namespace_handler.handleCommand(req);
        defer shard.namespace_handler.freeResult(result);
        sendNamespaceResponse(shard, conn, req.header.request_id, result);
    }

    // ── Core Command Logic ──────────────────────────────────────────────

    pub fn handleCommand(self: *NamespaceHandler, req: Request) CommandResult {
        const op: OpCode = @enumFromInt(req.header.op_code);
        return switch (op) {
            .namespace_create => self.handleCreate(req),
            .namespace_delete => self.handleDelete(req),
            .namespace_list => self.handleList(req),
            .namespace_info => self.handleInfo(req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown namespace opcode" } },
        };
    }

    // ── CREATE ──────────────────────────────────────────────────────────

    fn handleCreate(self: *NamespaceHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name is required" } };
        }
        if (name.len > MAX_NAMESPACE_LEN) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name too long" } };
        }
        if (!isValidName(name)) {
            return .{ .err = .{ .code = .invalid_request, .message = "invalid namespace name" } };
        }
        if (isReserved(name)) {
            return .{ .err = .{ .code = .invalid_request, .message = "reserved namespace name" } };
        }

        // Check if already exists
        if (self.namespaces.contains(name)) {
            return .{ .namespace_created = {} };
        }

        // Check capacity
        if (self.namespaces.count() >= MAX_NAMESPACES) {
            return .{ .err = .{ .code = .internal_error, .message = "namespace limit reached" } };
        }

        // Store owned copy of name
        const owned_name = self.allocator.dupe(u8, name) catch {
            return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        };
        errdefer self.allocator.free(owned_name);

        const now_ns: u64 = @intCast(@as(u64, @bitCast(@as(i64, std.time.milliTimestamp()))) * 1_000_000);

        self.namespaces.put(owned_name, .{
            .created_at_ns = now_ns,
        }) catch {
            self.allocator.free(owned_name);
            return .{ .err = .{ .code = .internal_error, .message = "namespace store failed" } };
        };

        return .{ .namespace_created = {} };
    }

    // ── DELETE ──────────────────────────────────────────────────────────

    fn handleDelete(self: *NamespaceHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name is required" } };
        }
        if (isReserved(name)) {
            return .{ .err = .{ .code = .invalid_request, .message = "cannot delete reserved namespace" } };
        }

        // "default" namespace cannot be deleted
        if (std.mem.eql(u8, name, "default")) {
            return .{ .err = .{ .code = .invalid_request, .message = "cannot delete default namespace" } };
        }

        if (self.namespaces.fetchRemove(name)) |kv| {
            self.allocator.free(kv.key);
            return .{ .namespace_deleted = {} };
        }

        // Idempotent — deleting non-existent namespace is OK
        return .{ .namespace_deleted = {} };
    }

    // ── LIST ────────────────────────────────────────────────────────────

    fn handleList(self: *NamespaceHandler, req: Request) CommandResult {
        // Check if system namespaces should be included
        const include_system = req.value.len > 0 and req.value[0] != 0;
        _ = include_system;

        const data = self.serializeNamespaceList() catch {
            return .{ .err = .{ .code = .internal_error, .message = "namespace list serialization failed" } };
        };

        return .{ .namespace_list = .{ .data = data, .allocated = true } };
    }

    // ── INFO ────────────────────────────────────────────────────────────

    fn handleInfo(self: *NamespaceHandler, req: Request) CommandResult {
        const name = req.key;

        if (name.len == 0) {
            return .{ .err = .{ .code = .invalid_request, .message = "namespace name is required" } };
        }

        const exists = self.namespaces.contains(name);

        // Duplicate the name for the response
        const owned_name = self.allocator.dupe(u8, name) catch {
            return .{ .err = .{ .code = .internal_error, .message = "allocation failed" } };
        };

        return .{ .namespace_info = .{
            .exists = exists,
            .name = owned_name,
            .allocated = true,
        } };
    }

    // ── Serialization ───────────────────────────────────────────────────

    /// Wire format: [count:u32] ([name_len:u16][name:bytes])*
    fn serializeNamespaceList(self: *NamespaceHandler) ![]u8 {
        // Calculate total size
        var total_size: usize = 4; // count header
        var entry_count: u32 = 0;
        var it = self.namespaces.iterator();
        while (it.next()) |entry| {
            total_size += 2 + entry.key_ptr.*.len; // u16 name_len + name bytes
            entry_count += 1;
        }

        const buf = try self.allocator.alloc(u8, total_size);
        errdefer self.allocator.free(buf);
        var offset: usize = 0;

        std.mem.writeInt(u32, buf[offset..][0..4], entry_count, .little);
        offset += 4;

        var it2 = self.namespaces.iterator();
        while (it2.next()) |entry| {
            const name = entry.key_ptr.*;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(name.len), .little);
            offset += 2;
            @memcpy(buf[offset .. offset + name.len], name);
            offset += name.len;
        }

        return buf;
    }

    // ── Validation ──────────────────────────────────────────────────────

    fn isValidName(name: []const u8) bool {
        if (name.len == 0) return false;
        for (name) |c| {
            switch (c) {
                'a'...'z', 'A'...'Z', '0'...'9', '-', '_', '.' => {},
                else => return false,
            }
        }
        // Must start with letter or underscore
        switch (name[0]) {
            'a'...'z', 'A'...'Z', '_' => return true,
            else => return false,
        }
    }

    fn isReserved(name: []const u8) bool {
        for (reserved_prefixes) |prefix| {
            if (name.len >= prefix.len and std.mem.eql(u8, name[0..prefix.len], prefix)) {
                // Exact match or prefix match with separator
                if (name.len == prefix.len) return true;
                if (name[prefix.len] == ':' or name[prefix.len] == '.') return true;
            }
        }
        return false;
    }

    // ── Free Result ─────────────────────────────────────────────────────

    pub fn freeResult(self: *NamespaceHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .namespace_list => |r| {
                if (r.allocated) self.allocator.free(r.data);
            },
            .namespace_info => |r| {
                if (r.allocated) self.allocator.free(r.name);
            },
            else => {},
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Response Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Map namespace CommandResult variants to wire responses.
fn sendNamespaceResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .ok, .namespace_created, .namespace_deleted => {
            shard.sendOkResponse(conn, request_id, "");
        },
        .err => |e| {
            shard.sendErrorResponse(conn, request_id, errorCodeToStatus(e.code), e.message);
        },
        .namespace_list => |n| {
            shard.sendOkResponse(conn, request_id, n.data);
        },
        .namespace_info => |n| {
            // Wire format: [exists:u8][name_len:u16 LE][name:bytes]
            var buf: [3 + 128]u8 = undefined;
            buf[0] = if (n.exists) 1 else 0;
            std.mem.writeInt(u16, buf[1..3], @intCast(n.name.len), .little);
            if (n.name.len > 0) {
                @memcpy(buf[3 .. 3 + n.name.len], n.name);
            }
            shard.sendOkResponse(conn, request_id, buf[0 .. 3 + n.name.len]);
        },
        else => {
            shard.sendErrorResponse(conn, request_id, .internal_error, "unhandled namespace response");
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
        else => .internal_error,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn makeRequest(op: OpCode, key: []const u8, value: []const u8) Request {
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
        .namespace = "",
        .key = key,
        .value = value,
        .options = "",
    };
}

test "namespace handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    NamespaceHandler.register(&dispatcher);

    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.namespace_create)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.namespace_delete)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.namespace_list)] != null);
    try testing.expect(dispatcher.handlers[@intFromEnum(OpCode.namespace_info)] != null);

    try testing.expectEqual(@as(u16, 4), dispatcher.handler_count);
}

test "namespace handler: create" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "production", ""));
    switch (result) {
        .namespace_created => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());
}

test "namespace handler: create idempotent" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "test-ns", ""));
    const result = handler.handleCommand(makeRequest(.namespace_create, "test-ns", ""));
    switch (result) {
        .namespace_created => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());
}

test "namespace handler: create empty name" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: create invalid name" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "has spaces", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: create reserved name" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_create, "_sys", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }

    const result2 = handler.handleCommand(makeRequest(.namespace_create, "_internal:test", ""));
    switch (result2) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: delete" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "staging", ""));
    try testing.expectEqual(@as(usize, 1), handler.namespaces.count());

    const result = handler.handleCommand(makeRequest(.namespace_delete, "staging", ""));
    switch (result) {
        .namespace_deleted => {},
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(usize, 0), handler.namespaces.count());
}

test "namespace handler: delete non-existent is idempotent" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_delete, "ghost", ""));
    switch (result) {
        .namespace_deleted => {},
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: delete default namespace blocked" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_delete, "default", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: delete reserved blocked" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_delete, "_flo.meta", ""));
    switch (result) {
        .err => |e| try testing.expectEqual(CommandResult.ErrorCode.invalid_request, e.code),
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: list" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "alpha", ""));
    _ = handler.handleCommand(makeRequest(.namespace_create, "beta", ""));

    const result = handler.handleCommand(makeRequest(.namespace_list, "", ""));
    switch (result) {
        .namespace_list => |r| {
            defer handler.freeResult(result);
            try testing.expect(r.allocated);
            const count_ns = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 2), count_ns);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: list empty" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_list, "", ""));
    switch (result) {
        .namespace_list => |r| {
            defer handler.freeResult(result);
            const count_ns = std.mem.readInt(u32, r.data[0..4], .little);
            try testing.expectEqual(@as(u32, 0), count_ns);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: info existing" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    _ = handler.handleCommand(makeRequest(.namespace_create, "myns", ""));

    const result = handler.handleCommand(makeRequest(.namespace_info, "myns", ""));
    switch (result) {
        .namespace_info => |r| {
            defer handler.freeResult(result);
            try testing.expect(r.exists);
            try testing.expectEqualStrings("myns", r.name);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: info non-existing" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    const result = handler.handleCommand(makeRequest(.namespace_info, "missing", ""));
    switch (result) {
        .namespace_info => |r| {
            defer handler.freeResult(result);
            try testing.expect(!r.exists);
            try testing.expectEqualStrings("missing", r.name);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "namespace handler: name validation" {
    // Valid names
    try testing.expect(NamespaceHandler.isValidName("production"));
    try testing.expect(NamespaceHandler.isValidName("test-env"));
    try testing.expect(NamespaceHandler.isValidName("stage_2"));
    try testing.expect(NamespaceHandler.isValidName("MyNs"));
    try testing.expect(NamespaceHandler.isValidName("_private"));
    try testing.expect(NamespaceHandler.isValidName("v1.0"));

    // Invalid names
    try testing.expect(!NamespaceHandler.isValidName(""));
    try testing.expect(!NamespaceHandler.isValidName("has spaces"));
    try testing.expect(!NamespaceHandler.isValidName("has/slash"));
    try testing.expect(!NamespaceHandler.isValidName("0starts-with-digit"));
    try testing.expect(!NamespaceHandler.isValidName("-starts-with-dash"));
}

test "namespace handler: reserved detection" {
    try testing.expect(NamespaceHandler.isReserved("_sys"));
    try testing.expect(NamespaceHandler.isReserved("_sys:meta"));
    try testing.expect(NamespaceHandler.isReserved("_sys.config"));
    try testing.expect(NamespaceHandler.isReserved("_internal"));
    try testing.expect(NamespaceHandler.isReserved("_flo"));
    try testing.expect(NamespaceHandler.isReserved("_flo:test"));

    try testing.expect(!NamespaceHandler.isReserved("_system"));
    try testing.expect(!NamespaceHandler.isReserved("production"));
    try testing.expect(!NamespaceHandler.isReserved("my_sys_ns"));
}

test "namespace handler: freeResult non-allocated is no-op" {
    const allocator = testing.allocator;
    var handler = NamespaceHandler.init(allocator);
    defer handler.deinit();

    handler.freeResult(.ok);
    handler.freeResult(.{ .err = .{ .code = .invalid_request, .message = "test" } });
    handler.freeResult(.{ .namespace_created = {} });
    handler.freeResult(.{ .namespace_deleted = {} });
}
