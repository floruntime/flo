//! RESP Protocol Parser and Serializer
//!
//! Implements RESP2/RESP3 (Redis Serialization Protocol) for redis-cli compatibility.
//! This allows Flo to be used with existing Redis clients and tools.
//!
//! RESPONSE ORDERING:
//! RESP requires IN-ORDER responses (unlike Flo-Proto which allows out-of-order).
//! If a later request completes before an earlier one, we must buffer until
//! we can send responses in the correct order.
//!
//! Supported RESP types:
//! - Simple Strings (+OK\r\n)
//! - Errors (-ERR message\r\n)
//! - Integers (:1234\r\n)
//! - Bulk Strings ($5\r\nhello\r\n)
//! - Arrays (*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n)
//! - Null Bulk String ($-1\r\n)
//! - Null Array (*-1\r\n)

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("proto.zig");
const CommandResult = @import("result.zig").CommandResult;

/// A RESP command translated to Flo wire protocol terms.
/// The RESP connection handler converts this to a proto.Request for the Dispatcher.
pub const RespCommand = struct {
    opcode: proto.OpCode,
    namespace: []const u8,
    key: []const u8,
    value: []const u8,
    ttl_ms: ?u64 = null,
    count: u32 = 1,
};

/// Header for stream record payloads
pub const Header = struct {
    key: []const u8,
    value: []const u8,
};

/// Record for stream appends
pub const Record = struct {
    payload: []const u8,
    headers: ?[]const Header = null,
};

/// RESP data types
pub const RespType = enum {
    simple_string,
    error_string,
    integer,
    bulk_string,
    array,
    null_bulk,
    null_array,
};

/// RESP value
pub const RespValue = union(enum) {
    simple_string: []const u8,
    error_string: []const u8,
    integer: i64,
    bulk_string: []const u8,
    array: []const RespValue,
    null_bulk: void,
    null_array: void,
};

/// Parser error types
pub const Error = error{
    IncompleteData,
    InvalidFormat,
    InvalidInteger,
    LineTooLong,
    UnknownType,
    NestingTooDeep,
    OutOfMemory,
};

/// Maximum line length for simple strings/errors
const MAX_LINE_LEN: usize = 64 * 1024;

/// Maximum nesting depth for arrays
const MAX_NESTING: usize = 8;

/// Parse result containing value and bytes consumed
pub const ParseResult = struct { value: RespValue, consumed: usize };

/// RESP parser (streaming, stateful)
pub const Parser = struct {
    allocator: Allocator,
    state: State,
    stack: std.ArrayListUnmanaged(StackFrame),

    const State = enum {
        start,
        simple_string,
        error_string,
        integer,
        bulk_len,
        bulk_data,
        array_len,
    };

    const StackFrame = struct {
        array: std.ArrayListUnmanaged(RespValue),
        remaining: usize,
    };

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .state = .start,
            .stack = .empty,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.stack.items) |*frame| {
            for (frame.array.items) |*val| {
                freeValue(self.allocator, val);
            }
            frame.array.deinit(self.allocator);
        }
        self.stack.deinit(self.allocator);
    }

    pub fn reset(self: *Self) void {
        for (self.stack.items) |*frame| {
            for (frame.array.items) |*val| {
                freeValue(self.allocator, val);
            }
            frame.array.deinit(self.allocator);
        }
        self.stack.clearRetainingCapacity();
        self.state = .start;
    }

    /// Parse data and return result
    /// Returns the parsed value and bytes consumed, or null if incomplete
    pub fn parse(self: *Self, data: []const u8) Error!?ParseResult {
        return self.parseInternal(data, 0);
    }

    fn parseInternal(self: *Self, data: []const u8, depth: usize) Error!?ParseResult {
        if (depth > MAX_NESTING) return Error.NestingTooDeep;
        if (data.len == 0) return null;

        var offset: usize = 0;

        switch (data[0]) {
            '+' => {
                // Simple string
                const line_end = std.mem.indexOf(u8, data[1..], "\r\n") orelse return null;
                if (line_end > MAX_LINE_LEN) return Error.LineTooLong;
                const str = data[1 .. 1 + line_end];
                const str_copy = self.allocator.dupe(u8, str) catch return Error.OutOfMemory;
                return .{
                    .value = .{ .simple_string = str_copy },
                    .consumed = 1 + line_end + 2,
                };
            },
            '-' => {
                // Error string
                const line_end = std.mem.indexOf(u8, data[1..], "\r\n") orelse return null;
                if (line_end > MAX_LINE_LEN) return Error.LineTooLong;
                const str = data[1 .. 1 + line_end];
                const str_copy = self.allocator.dupe(u8, str) catch return Error.OutOfMemory;
                return .{
                    .value = .{ .error_string = str_copy },
                    .consumed = 1 + line_end + 2,
                };
            },
            ':' => {
                // Integer
                const line_end = std.mem.indexOf(u8, data[1..], "\r\n") orelse return null;
                const int_str = data[1 .. 1 + line_end];
                const value = std.fmt.parseInt(i64, int_str, 10) catch return Error.InvalidInteger;
                return .{
                    .value = .{ .integer = value },
                    .consumed = 1 + line_end + 2,
                };
            },
            '$' => {
                // Bulk string
                const line_end = std.mem.indexOf(u8, data[1..], "\r\n") orelse return null;
                const len_str = data[1 .. 1 + line_end];
                const len = std.fmt.parseInt(i64, len_str, 10) catch return Error.InvalidInteger;

                if (len < 0) {
                    return .{
                        .value = .{ .null_bulk = {} },
                        .consumed = 1 + line_end + 2,
                    };
                }

                const bulk_start = 1 + line_end + 2;
                const bulk_end = bulk_start + @as(usize, @intCast(len));
                if (data.len < bulk_end + 2) return null;

                const bulk = data[bulk_start..bulk_end];
                const bulk_copy = self.allocator.dupe(u8, bulk) catch return Error.OutOfMemory;
                return .{
                    .value = .{ .bulk_string = bulk_copy },
                    .consumed = bulk_end + 2,
                };
            },
            '*' => {
                // Array
                const line_end = std.mem.indexOf(u8, data[1..], "\r\n") orelse return null;
                const count_str = data[1 .. 1 + line_end];
                const count = std.fmt.parseInt(i64, count_str, 10) catch return Error.InvalidInteger;

                if (count < 0) {
                    return .{
                        .value = .{ .null_array = {} },
                        .consumed = 1 + line_end + 2,
                    };
                }

                offset = 1 + line_end + 2;
                const element_count: usize = @intCast(count);

                var elements: std.ArrayListUnmanaged(RespValue) = .empty;
                errdefer {
                    for (elements.items) |*v| freeValue(self.allocator, v);
                    elements.deinit(self.allocator);
                }

                for (0..element_count) |_| {
                    const result = try self.parseInternal(data[offset..], depth + 1) orelse {
                        // Incomplete - cleanup and return
                        for (elements.items) |*v| freeValue(self.allocator, v);
                        elements.deinit(self.allocator);
                        return null;
                    };
                    elements.append(self.allocator, result.value) catch return Error.OutOfMemory;
                    offset += result.consumed;
                }

                const arr = elements.toOwnedSlice(self.allocator) catch return Error.OutOfMemory;
                return .{
                    .value = .{ .array = arr },
                    .consumed = offset,
                };
            },
            else => return Error.UnknownType,
        }
    }
};

/// Free a RESP value and all nested values
pub fn freeValue(allocator: Allocator, value: *RespValue) void {
    switch (value.*) {
        .simple_string, .error_string, .bulk_string => |s| allocator.free(s),
        .array => |arr| {
            for (arr) |*v| {
                var v_copy = v.*;
                freeValue(allocator, &v_copy);
            }
            allocator.free(arr);
        },
        .null_bulk, .null_array, .integer => {},
    }
}

/// Serialize a RESP value to bytes
pub fn serialize(allocator: Allocator, value: RespValue) ![]u8 {
    var buffer: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buffer.deinit(allocator);

    try serializeInto(allocator, &buffer, value);
    return buffer.toOwnedSlice(allocator);
}

fn serializeInto(allocator: Allocator, buffer: *std.ArrayListUnmanaged(u8), value: RespValue) !void {
    switch (value) {
        .simple_string => |s| {
            try buffer.append(allocator, '+');
            try buffer.appendSlice(allocator, s);
            try buffer.appendSlice(allocator, "\r\n");
        },
        .error_string => |s| {
            try buffer.append(allocator, '-');
            try buffer.appendSlice(allocator, s);
            try buffer.appendSlice(allocator, "\r\n");
        },
        .integer => |i| {
            try buffer.append(allocator, ':');
            var buf: [32]u8 = undefined;
            const str = std.fmt.bufPrint(&buf, "{d}", .{i}) catch unreachable;
            try buffer.appendSlice(allocator, str);
            try buffer.appendSlice(allocator, "\r\n");
        },
        .bulk_string => |s| {
            try buffer.append(allocator, '$');
            var buf: [32]u8 = undefined;
            const len_str = std.fmt.bufPrint(&buf, "{d}", .{s.len}) catch unreachable;
            try buffer.appendSlice(allocator, len_str);
            try buffer.appendSlice(allocator, "\r\n");
            try buffer.appendSlice(allocator, s);
            try buffer.appendSlice(allocator, "\r\n");
        },
        .array => |arr| {
            try buffer.append(allocator, '*');
            var buf: [32]u8 = undefined;
            const len_str = std.fmt.bufPrint(&buf, "{d}", .{arr.len}) catch unreachable;
            try buffer.appendSlice(allocator, len_str);
            try buffer.appendSlice(allocator, "\r\n");
            for (arr) |elem| {
                try serializeInto(allocator, buffer, elem);
            }
        },
        .null_bulk => {
            try buffer.appendSlice(allocator, "$-1\r\n");
        },
        .null_array => {
            try buffer.appendSlice(allocator, "*-1\r\n");
        },
    }
}

// =============================================================================
// Command Translation (RESP -> Flo Command)
// =============================================================================

/// Result of USE command - changes session namespace
pub const UseResult = struct {
    namespace: []const u8,
};

/// Result of translating a RESP command
pub const TranslateResult = union(enum) {
    command: RespCommand,
    use_namespace: UseResult,
    select_db: u32, // SELECT db_num (Redis compatibility)
};

/// Translate a RESP command array to a Flo RespCommand (with namespace)
/// Pass current session namespace. For USE command, returns UseResult instead.
pub fn translateCommand(allocator: Allocator, value: RespValue, namespace: []const u8) !TranslateResult {
    const arr = switch (value) {
        .array => |a| a,
        else => return error.InvalidCommand,
    };

    if (arr.len == 0) return error.InvalidCommand;

    // Get command name (first element)
    const cmd_name = switch (arr[0]) {
        .bulk_string => |s| s,
        .simple_string => |s| s,
        else => return error.InvalidCommand,
    };

    // Uppercase for comparison
    var cmd_upper: [32]u8 = undefined;
    const cmd_len = @min(cmd_name.len, 32);
    for (cmd_name[0..cmd_len], 0..) |c, i| {
        cmd_upper[i] = std.ascii.toUpper(c);
    }
    const cmd = cmd_upper[0..cmd_len];

    // Handle USE command for namespace selection
    if (std.mem.eql(u8, cmd, "USE")) {
        if (arr.len < 2) return error.InvalidCommand;
        const ns = getBulkString(arr[1]) orelse return error.InvalidCommand;
        return .{ .use_namespace = .{ .namespace = ns } };
    }

    // Handle SELECT for Redis compatibility (maps to namespace)
    if (std.mem.eql(u8, cmd, "SELECT")) {
        if (arr.len < 2) return error.InvalidCommand;
        const db_num = getInteger(arr[1]) orelse return error.InvalidCommand;
        return .{ .select_db = @intCast(db_num) };
    }

    // Match commands
    if (std.mem.eql(u8, cmd, "PING")) {
        return .{ .command = .{
            .opcode = .ping,
            .namespace = namespace,
            .key = "",
            .value = "",
        } };
    } else if (std.mem.eql(u8, cmd, "GET")) {
        if (arr.len < 2) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        return .{ .command = .{
            .opcode = .kv_get,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = "",
        } };
    } else if (std.mem.eql(u8, cmd, "SET")) {
        if (arr.len < 3) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        const val = getBulkString(arr[2]) orelse return error.InvalidCommand;

        var ttl_ms: ?u64 = null;

        // Parse optional arguments (EX, PX, etc.)
        var i: usize = 3;
        while (i < arr.len) : (i += 1) {
            const opt = getBulkString(arr[i]) orelse continue;
            if (std.ascii.eqlIgnoreCase(opt, "EX") and i + 1 < arr.len) {
                const secs = getInteger(arr[i + 1]) orelse continue;
                ttl_ms = @intCast(secs * 1000);
                i += 1;
            } else if (std.ascii.eqlIgnoreCase(opt, "PX") and i + 1 < arr.len) {
                const ms = getInteger(arr[i + 1]) orelse continue;
                ttl_ms = @intCast(ms);
                i += 1;
            }
        }

        return .{ .command = .{
            .opcode = .kv_put,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = try allocator.dupe(u8, val),
            .ttl_ms = ttl_ms,
        } };
    } else if (std.mem.eql(u8, cmd, "DEL")) {
        if (arr.len < 2) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        return .{ .command = .{
            .opcode = .kv_delete,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = "",
        } };
    } else if (std.mem.eql(u8, cmd, "XADD")) {
        if (arr.len < 4) return error.InvalidCommand;
        const stream_name = getBulkString(arr[1]) orelse return error.InvalidCommand;
        // arr[2] is the ID (* for auto-generate)
        // Remaining are field-value pairs concatenated as payload
        var payload: std.ArrayListUnmanaged(u8) = .empty;
        defer payload.deinit(allocator);

        var j: usize = 3;
        while (j + 1 < arr.len) : (j += 2) {
            const field = getBulkString(arr[j]) orelse continue;
            const val = getBulkString(arr[j + 1]) orelse continue;
            try payload.appendSlice(allocator, field);
            try payload.append(allocator, '=');
            try payload.appendSlice(allocator, val);
            try payload.append(allocator, '\n');
        }

        return .{ .command = .{
            .opcode = .stream_append,
            .namespace = namespace,
            .key = try allocator.dupe(u8, stream_name),
            .value = try payload.toOwnedSlice(allocator),
        } };
    } else if (std.mem.eql(u8, cmd, "XREAD")) {
        // XREAD [COUNT count] [BLOCK ms] STREAMS stream [stream...] id [id...]
        // Simplified: just support single stream
        var count: u32 = 100;
        var stream_idx: usize = 0;

        var k: usize = 1;
        while (k < arr.len) : (k += 1) {
            const arg = getBulkString(arr[k]) orelse continue;
            if (std.ascii.eqlIgnoreCase(arg, "COUNT") and k + 1 < arr.len) {
                count = @intCast(getInteger(arr[k + 1]) orelse 100);
                k += 1;
            } else if (std.ascii.eqlIgnoreCase(arg, "BLOCK") and k + 1 < arr.len) {
                // Block timeout parsed but stored in count for now
                _ = getInteger(arr[k + 1]);
                k += 1;
            } else if (std.ascii.eqlIgnoreCase(arg, "STREAMS")) {
                stream_idx = k + 1;
                break;
            }
        }

        if (stream_idx == 0 or stream_idx >= arr.len) return error.InvalidCommand;
        const stream_name = getBulkString(arr[stream_idx]) orelse return error.InvalidCommand;

        return .{ .command = .{
            .opcode = .stream_read,
            .namespace = namespace,
            .key = try allocator.dupe(u8, stream_name),
            .value = "",
            .count = count,
        } };
    } else if (std.mem.eql(u8, cmd, "LPUSH") or std.mem.eql(u8, cmd, "RPUSH")) {
        // Map Redis list push to Flo queue enqueue
        if (arr.len < 3) return error.InvalidCommand;
        const queue_name = getBulkString(arr[1]) orelse return error.InvalidCommand;
        const val = getBulkString(arr[2]) orelse return error.InvalidCommand;
        return .{ .command = .{
            .opcode = .queue_enqueue,
            .namespace = namespace,
            .key = try allocator.dupe(u8, queue_name),
            .value = try allocator.dupe(u8, val),
        } };
    } else if (std.mem.eql(u8, cmd, "LPOP") or std.mem.eql(u8, cmd, "RPOP")) {
        // Map Redis list pop to Flo queue dequeue
        if (arr.len < 2) return error.InvalidCommand;
        const queue_name = getBulkString(arr[1]) orelse return error.InvalidCommand;
        return .{ .command = .{
            .opcode = .queue_dequeue,
            .namespace = namespace,
            .key = try allocator.dupe(u8, queue_name),
            .value = "",
        } };
    } else if (std.mem.eql(u8, cmd, "INCR") or std.mem.eql(u8, cmd, "DECR")) {
        if (arr.len < 2) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        const delta: i64 = if (std.mem.eql(u8, cmd, "INCR")) 1 else -1;
        const buf = try allocator.alloc(u8, 8);
        std.mem.writeInt(i64, buf[0..8], delta, .little);
        return .{ .command = .{
            .opcode = .kv_incr,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = buf,
        } };
    } else if (std.mem.eql(u8, cmd, "INCRBY") or std.mem.eql(u8, cmd, "DECRBY")) {
        if (arr.len < 3) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        const raw = getInteger(arr[2]) orelse return error.InvalidCommand;
        const delta: i64 = if (std.mem.eql(u8, cmd, "INCRBY")) raw else -raw;
        const buf = try allocator.alloc(u8, 8);
        std.mem.writeInt(i64, buf[0..8], delta, .little);
        return .{ .command = .{
            .opcode = .kv_incr,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = buf,
        } };
    } else if (std.mem.eql(u8, cmd, "EXPIRE") or std.mem.eql(u8, cmd, "PEXPIRE")) {
        if (arr.len < 3) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        const raw = getInteger(arr[2]) orelse return error.InvalidCommand;
        if (raw < 0) return error.InvalidCommand;
        const ttl_seconds: u64 = if (std.mem.eql(u8, cmd, "EXPIRE"))
            @intCast(raw)
        else
            @intCast(@divTrunc(raw, 1000));
        const buf = try allocator.alloc(u8, 8);
        std.mem.writeInt(u64, buf[0..8], ttl_seconds, .little);
        return .{ .command = .{
            .opcode = .kv_touch,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = buf,
        } };
    } else if (std.mem.eql(u8, cmd, "PERSIST")) {
        if (arr.len < 2) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        return .{ .command = .{
            .opcode = .kv_persist,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = "",
        } };
    } else if (std.mem.eql(u8, cmd, "EXISTS")) {
        if (arr.len < 2) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        return .{ .command = .{
            .opcode = .kv_exists,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = "",
        } };
    } else if (std.ascii.eqlIgnoreCase(cmd_name, "JSON.GET")) {
        if (arr.len < 2) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        const path: []const u8 = if (arr.len >= 3) (getBulkString(arr[2]) orelse "$") else "$";
        return .{ .command = .{
            .opcode = .kv_json_get,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = try allocator.dupe(u8, path),
        } };
    } else if (std.ascii.eqlIgnoreCase(cmd_name, "JSON.SET")) {
        if (arr.len < 4) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        const path = getBulkString(arr[2]) orelse return error.InvalidCommand;
        const json = getBulkString(arr[3]) orelse return error.InvalidCommand;
        if (path.len > std.math.maxInt(u16)) return error.InvalidCommand;
        const buf = try allocator.alloc(u8, 2 + path.len + json.len);
        std.mem.writeInt(u16, buf[0..2], @intCast(path.len), .little);
        @memcpy(buf[2 .. 2 + path.len], path);
        @memcpy(buf[2 + path.len ..], json);
        return .{ .command = .{
            .opcode = .kv_json_set,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = buf,
        } };
    } else if (std.ascii.eqlIgnoreCase(cmd_name, "JSON.DEL") or std.ascii.eqlIgnoreCase(cmd_name, "JSON.FORGET")) {
        if (arr.len < 2) return error.InvalidCommand;
        const key = getBulkString(arr[1]) orelse return error.InvalidCommand;
        const path: []const u8 = if (arr.len >= 3) (getBulkString(arr[2]) orelse "$") else "$";
        return .{ .command = .{
            .opcode = .kv_json_del,
            .namespace = namespace,
            .key = try allocator.dupe(u8, key),
            .value = try allocator.dupe(u8, path),
        } };
    } else {
        return error.UnknownCommand;
    }
}

fn getBulkString(value: RespValue) ?[]const u8 {
    return switch (value) {
        .bulk_string => |s| s,
        .simple_string => |s| s,
        else => null,
    };
}

fn getInteger(value: RespValue) ?i64 {
    return switch (value) {
        .integer => |i| i,
        .bulk_string => |s| std.fmt.parseInt(i64, s, 10) catch null,
        else => null,
    };
}

// =============================================================================
// Result Translation (Flo Result -> RESP)
// =============================================================================

/// Translate a Flo CommandResult to RESP value
pub fn translateResult(result: CommandResult) RespValue {
    return switch (result) {
        .ok => .{ .simple_string = "OK" },
        .pong => .{ .simple_string = "PONG" },
        .err => |e| .{ .error_string = e.message },

        .kv_value => |v| blk: {
            // INCR/INCRBY/DECR/DECRBY return 8-byte i64 LE — translate to RESP integer.
            // EXISTS returns 1-byte — translate to RESP integer.
            if (v.value.len == 8) {
                break :blk .{ .integer = std.mem.readInt(i64, v.value[0..8], .little) };
            }
            if (v.value.len == 1 and (v.value[0] == 0 or v.value[0] == 1)) {
                break :blk .{ .integer = @intCast(v.value[0]) };
            }
            break :blk .{ .bulk_string = v.value };
        },
        .kv_not_found => .{ .null_bulk = {} },
        .kv_put_ok => .{ .simple_string = "OK" },
        .kv_cas_failed => .{ .error_string = "ERR CAS version mismatch" },
        .kv_condition_not_met => .{ .null_bulk = {} },
        .kv_scan_result => |r| blk: {
            // Used by JSON.GET — payload is the JSON bytes.
            if (r.data.len == 0) break :blk .{ .null_bulk = {} };
            break :blk .{ .bulk_string = r.data };
        },

        .stream_append_ok => |a| .{ .integer = @intCast(a.sequence) },
        .stream_messages => |m| blk: {
            // Return pre-serialized message data as bulk string.
            // Wire format: [count:u32]([sequence:u64][timestamp_ms:i64][tier:u8]
            // [partition:u32][key_present:u8][payload_len:u32][payload][header_count:u32])*
            if (m.data.len == 0) break :blk .{ .null_array = {} };
            break :blk .{ .bulk_string = m.data };
        },

        else => .{ .null_bulk = {} },
    };
}

// =============================================================================
// Tests
// =============================================================================

test "parse simple string" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("+OK\r\n")).?;
    try std.testing.expectEqualStrings("OK", result.value.simple_string);
    try std.testing.expectEqual(@as(usize, 5), result.consumed);

    freeValue(allocator, &result.value);
}

test "parse error string" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("-ERR unknown command\r\n")).?;
    try std.testing.expectEqualStrings("ERR unknown command", result.value.error_string);

    freeValue(allocator, &result.value);
}

test "parse integer" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const result = (try parser.parse(":12345\r\n")).?;
    try std.testing.expectEqual(@as(i64, 12345), result.value.integer);
}

test "parse negative integer" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const result = (try parser.parse(":-100\r\n")).?;
    try std.testing.expectEqual(@as(i64, -100), result.value.integer);
}

test "parse bulk string" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("$5\r\nhello\r\n")).?;
    try std.testing.expectEqualStrings("hello", result.value.bulk_string);

    freeValue(allocator, &result.value);
}

test "parse null bulk string" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const result = (try parser.parse("$-1\r\n")).?;
    try std.testing.expect(result.value == .null_bulk);
}

test "parse array" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")).?;
    try std.testing.expectEqual(@as(usize, 2), result.value.array.len);
    try std.testing.expectEqualStrings("GET", result.value.array[0].bulk_string);
    try std.testing.expectEqualStrings("foo", result.value.array[1].bulk_string);

    freeValue(allocator, &result.value);
}

test "serialize simple string" {
    const allocator = std.testing.allocator;
    const data = try serialize(allocator, .{ .simple_string = "OK" });
    defer allocator.free(data);
    try std.testing.expectEqualStrings("+OK\r\n", data);
}

test "serialize bulk string" {
    const allocator = std.testing.allocator;
    const data = try serialize(allocator, .{ .bulk_string = "hello" });
    defer allocator.free(data);
    try std.testing.expectEqualStrings("$5\r\nhello\r\n", data);
}

test "translateCommand PING" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*1\r\n$4\r\nPING\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    try std.testing.expectEqual(proto.OpCode.ping, translated.command.opcode);
}

test "translateCommand GET" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);

    try std.testing.expectEqual(proto.OpCode.kv_get, cmd.opcode);
    try std.testing.expectEqualStrings("foo", cmd.key);
    try std.testing.expectEqualStrings("default", cmd.namespace);
}

test "translateCommand SET with TTL" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nEX\r\n$2\r\n60\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "myns");
    const cmd = translated.command;
    defer allocator.free(cmd.key);
    defer allocator.free(cmd.value);

    try std.testing.expectEqual(proto.OpCode.kv_put, cmd.opcode);
    try std.testing.expectEqualStrings("foo", cmd.key);
    try std.testing.expectEqualStrings("bar", cmd.value);
    try std.testing.expectEqual(@as(?u64, 60000), cmd.ttl_ms); // 60 * 1000
}

test "translateCommand DEL" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);

    try std.testing.expectEqual(proto.OpCode.kv_delete, cmd.opcode);
    try std.testing.expectEqualStrings("mykey", cmd.key);
}

test "translateCommand USE namespace" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*2\r\n$3\r\nUSE\r\n$4\r\nprod\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    try std.testing.expectEqualStrings("prod", translated.use_namespace.namespace);
}

test "translateCommand LPUSH maps to queue enqueue" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*3\r\n$5\r\nLPUSH\r\n$5\r\ntasks\r\n$4\r\nwork\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);
    defer allocator.free(cmd.value);

    try std.testing.expectEqual(proto.OpCode.queue_enqueue, cmd.opcode);
    try std.testing.expectEqualStrings("tasks", cmd.key);
    try std.testing.expectEqualStrings("work", cmd.value);
}

test "translateCommand INCR encodes 8-byte LE +1" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*2\r\n$4\r\nINCR\r\n$3\r\ncnt\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);
    defer allocator.free(cmd.value);

    try std.testing.expectEqual(proto.OpCode.kv_incr, cmd.opcode);
    try std.testing.expectEqualStrings("cnt", cmd.key);
    try std.testing.expectEqual(@as(usize, 8), cmd.value.len);
    try std.testing.expectEqual(@as(i64, 1), std.mem.readInt(i64, cmd.value[0..8], .little));
}

test "translateCommand INCRBY encodes signed delta" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*3\r\n$6\r\nINCRBY\r\n$3\r\ncnt\r\n$2\r\n42\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);
    defer allocator.free(cmd.value);

    try std.testing.expectEqual(proto.OpCode.kv_incr, cmd.opcode);
    try std.testing.expectEqual(@as(i64, 42), std.mem.readInt(i64, cmd.value[0..8], .little));
}

test "translateCommand DECR encodes -1" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*2\r\n$4\r\nDECR\r\n$3\r\ncnt\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);
    defer allocator.free(cmd.value);

    try std.testing.expectEqual(proto.OpCode.kv_incr, cmd.opcode);
    try std.testing.expectEqual(@as(i64, -1), std.mem.readInt(i64, cmd.value[0..8], .little));
}

test "translateCommand EXPIRE encodes seconds as u64 LE" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*3\r\n$6\r\nEXPIRE\r\n$3\r\nfoo\r\n$2\r\n60\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);
    defer allocator.free(cmd.value);

    try std.testing.expectEqual(proto.OpCode.kv_touch, cmd.opcode);
    try std.testing.expectEqual(@as(u64, 60), std.mem.readInt(u64, cmd.value[0..8], .little));
}

test "translateCommand PEXPIRE converts ms to seconds" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*3\r\n$7\r\nPEXPIRE\r\n$3\r\nfoo\r\n$5\r\n60000\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);
    defer allocator.free(cmd.value);

    try std.testing.expectEqual(proto.OpCode.kv_touch, cmd.opcode);
    try std.testing.expectEqual(@as(u64, 60), std.mem.readInt(u64, cmd.value[0..8], .little));
}

test "translateCommand PERSIST" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*2\r\n$7\r\nPERSIST\r\n$3\r\nfoo\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);

    try std.testing.expectEqual(proto.OpCode.kv_persist, cmd.opcode);
    try std.testing.expectEqualStrings("foo", cmd.key);
}

test "translateCommand EXISTS" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);

    try std.testing.expectEqual(proto.OpCode.kv_exists, cmd.opcode);
}

test "translateCommand JSON.SET encodes [path_len:u16][path][json]" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*4\r\n$8\r\nJSON.SET\r\n$3\r\ndoc\r\n$1\r\n$\r\n$12\r\n{\"name\":\"x\"}\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);
    defer allocator.free(cmd.value);

    try std.testing.expectEqual(proto.OpCode.kv_json_set, cmd.opcode);
    const path_len = std.mem.readInt(u16, cmd.value[0..2], .little);
    try std.testing.expectEqual(@as(u16, 1), path_len);
    try std.testing.expectEqualStrings("$", cmd.value[2 .. 2 + path_len]);
    try std.testing.expectEqualStrings("{\"name\":\"x\"}", cmd.value[2 + path_len ..]);
}

test "translateCommand JSON.GET path defaults to $" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*2\r\n$8\r\nJSON.GET\r\n$3\r\ndoc\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);
    defer allocator.free(cmd.value);

    try std.testing.expectEqual(proto.OpCode.kv_json_get, cmd.opcode);
    try std.testing.expectEqualStrings("$", cmd.value);
}

test "translateCommand JSON.DEL" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    var result = (try parser.parse("*3\r\n$8\r\nJSON.DEL\r\n$3\r\ndoc\r\n$5\r\n$.foo\r\n")).?;
    defer freeValue(allocator, &result.value);

    const translated = try translateCommand(allocator, result.value, "default");
    const cmd = translated.command;
    defer allocator.free(cmd.key);
    defer allocator.free(cmd.value);

    try std.testing.expectEqual(proto.OpCode.kv_json_del, cmd.opcode);
    try std.testing.expectEqualStrings("$.foo", cmd.value);
}
