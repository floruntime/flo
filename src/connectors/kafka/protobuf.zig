//! Protobuf Binary Decoder → JSON
//!
//! Decodes Protocol Buffers wire-format data into JSON. For use with
//! Confluent Schema Registry Protobuf subjects.
//!
//! Confluent Protobuf wire format:
//!   [0x00][schema_id: i32 BE][msg_index_count: varint][msg_indexes: varint...][protobuf_bytes]
//!
//! Since we don't have the .proto schema at decode time (schema registry returns
//! the proto definition as text), we use a self-describing approach:
//! decode protobuf wire format using field numbers as JSON keys and raw values.
//!
//! Wire types:
//!   0 = varint, 1 = 64-bit, 2 = length-delimited, 5 = 32-bit
//!
//! Output format: {"1": value, "2": "string", "3": {...}, ...}
//! Field numbers are used as keys since we don't have field name mappings
//! without a compiled .proto schema descriptor.

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;

// =============================================================================
// Protobuf Wire Types
// =============================================================================

pub const WireType = enum(u3) {
    varint = 0,
    fixed64 = 1,
    length_delimited = 2,
    // 3, 4 = start/end group (deprecated)
    fixed32 = 5,
    _,
};

// =============================================================================
// Confluent Protobuf Header
// =============================================================================

/// Parse the Confluent protobuf wire format header.
/// Returns the message indexes and the offset where protobuf data begins.
pub fn parseConfluentProtobufHeader(data: []const u8) ?struct {
    payload: []const u8,
} {
    if (data.len < 6) return null;
    if (data[0] != 0x00) return null;

    // Skip schema_id (4 bytes)
    var pos: usize = 5;

    // Read message index count
    const index_count = readVarint(data, &pos) orelse return null;

    // Skip message indexes
    var i: u64 = 0;
    while (i < index_count) : (i += 1) {
        _ = readVarint(data, &pos) orelse return null;
    }

    if (pos > data.len) return null;
    return .{ .payload = data[pos..] };
}

// =============================================================================
// Protobuf Decoder → JSON
// =============================================================================

pub const ProtobufDecoder = struct {
    allocator: Allocator,

    pub fn init(allocator: Allocator) ProtobufDecoder {
        return .{ .allocator = allocator };
    }

    /// Decode protobuf wire-format bytes into a JSON object.
    /// Field numbers become string keys, values are decoded per wire type.
    pub fn decode(self: *ProtobufDecoder, data: []const u8) ![]const u8 {
        var output: std.ArrayList(u8) = .{};
        errdefer output.deinit(self.allocator);

        try self.decodeMessage(data, &output, 0);

        return output.toOwnedSlice(self.allocator);
    }

    fn decodeMessage(
        self: *ProtobufDecoder,
        data: []const u8,
        output: *std.ArrayList(u8),
        depth: usize,
    ) !void {
        if (depth > 32) return error.ProtobufDecodeFailed; // nesting limit

        try output.append(self.allocator, '{');
        var pos: usize = 0;
        var first = true;

        while (pos < data.len) {
            // Read field tag: (field_number << 3) | wire_type
            const tag = readVarint(data, &pos) orelse break;
            const field_number = tag >> 3;
            const wire_type: u3 = @intCast(tag & 0x07);

            if (field_number == 0) break; // invalid

            if (!first) try output.append(self.allocator, ',');
            first = false;

            // Write field number as key
            try output.append(self.allocator, '"');
            var num_buf: [20]u8 = undefined;
            const num_str = std.fmt.bufPrint(&num_buf, "{d}", .{field_number}) catch break;
            try output.appendSlice(self.allocator, num_str);
            try output.append(self.allocator, '"');
            try output.append(self.allocator, ':');

            switch (@as(WireType, @enumFromInt(wire_type))) {
                .varint => {
                    const v = readVarint(data, &pos) orelse return error.ProtobufDecodeFailed;
                    // Decode as signed (zigzag) if it looks like a negative value,
                    // otherwise as unsigned — we can't know without schema
                    var val_buf: [21]u8 = undefined;
                    const val_str = std.fmt.bufPrint(&val_buf, "{d}", .{v}) catch unreachable;
                    try output.appendSlice(self.allocator, val_str);
                },
                .fixed64 => {
                    if (pos + 8 > data.len) return error.ProtobufDecodeFailed;
                    const v = std.mem.readInt(u64, data[pos..][0..8], .little);
                    pos += 8;
                    var val_buf: [21]u8 = undefined;
                    const val_str = std.fmt.bufPrint(&val_buf, "{d}", .{v}) catch unreachable;
                    try output.appendSlice(self.allocator, val_str);
                },
                .length_delimited => {
                    const len_raw = readVarint(data, &pos) orelse return error.ProtobufDecodeFailed;
                    const len: usize = @intCast(len_raw);
                    if (pos + len > data.len) return error.ProtobufDecodeFailed;
                    const field_data = data[pos..][0..len];
                    pos += len;

                    // Heuristic: try to decode as a nested message first.
                    // If it fails, treat as a UTF-8 string (or base64 for binary).
                    if (len > 0 and looksLikeProtobuf(field_data)) {
                        self.decodeMessage(field_data, output, depth + 1) catch {
                            // Fall back to string
                            try appendJsonStringOrBase64(self.allocator, output, field_data);
                        };
                    } else {
                        try appendJsonStringOrBase64(self.allocator, output, field_data);
                    }
                },
                .fixed32 => {
                    if (pos + 4 > data.len) return error.ProtobufDecodeFailed;
                    const v = std.mem.readInt(u32, data[pos..][0..4], .little);
                    pos += 4;
                    var val_buf: [11]u8 = undefined;
                    const val_str = std.fmt.bufPrint(&val_buf, "{d}", .{v}) catch unreachable;
                    try output.appendSlice(self.allocator, val_str);
                },
                _ => {
                    // Unknown wire type — skip (can't determine size, abort)
                    return error.ProtobufDecodeFailed;
                },
            }
        }

        try output.append(self.allocator, '}');
    }
};

// =============================================================================
// Helpers
// =============================================================================

/// Read a protobuf varint (unsigned, base-128).
fn readVarint(data: []const u8, pos: *usize) ?u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    while (pos.* < data.len) {
        const b = data[pos.*];
        pos.* += 1;
        result |= @as(u64, b & 0x7F) << shift;
        if (b & 0x80 == 0) return result;
        shift = std.math.add(u6, shift, 7) catch return null;
    }
    return null;
}

/// Heuristic: check if data looks like a valid protobuf message.
/// Tries to fully consume the data as a sequence of valid protobuf fields.
/// If it parses cleanly with no leftover bytes and has reasonable field numbers,
/// it's likely protobuf. Requires field numbers to be in ascending order
/// (a common protobuf convention) and the first field to start at a low number.
fn looksLikeProtobuf(data: []const u8) bool {
    if (data.len < 2) return false;
    var pos: usize = 0;
    var field_count: usize = 0;
    var prev_field: u64 = 0;

    while (pos < data.len) {
        const tag = readVarint(data, &pos) orelse return false;
        const field_number = tag >> 3;
        const wire_type: u3 = @intCast(tag & 0x07);

        if (field_number == 0 or field_number > 1000) return false;

        // First field must have a low number (nested messages start at 1-8)
        if (field_count == 0 and field_number > 8) return false;

        // Require ascending field numbers (may repeat for repeated fields)
        if (field_number < prev_field) return false;
        prev_field = field_number;

        // Skip over the field value
        switch (@as(WireType, @enumFromInt(wire_type))) {
            .varint => {
                _ = readVarint(data, &pos) orelse return false;
            },
            .fixed64 => {
                if (pos + 8 > data.len) return false;
                pos += 8;
            },
            .length_delimited => {
                const len_raw = readVarint(data, &pos) orelse return false;
                const len: usize = @intCast(len_raw);
                if (pos + len > data.len) return false;
                pos += len;
            },
            .fixed32 => {
                if (pos + 4 > data.len) return false;
                pos += 4;
            },
            _ => return false,
        }
        field_count += 1;
    }

    // Must have consumed exactly all bytes AND have at least one field
    return pos == data.len and field_count > 0;
}

/// Output a JSON string if valid UTF-8, otherwise base64-encode.
fn appendJsonStringOrBase64(allocator: Allocator, output: *std.ArrayList(u8), data: []const u8) !void {
    if (std.unicode.utf8ValidateSlice(data)) {
        try output.append(allocator, '"');
        for (data) |c| {
            switch (c) {
                '"' => try output.appendSlice(allocator, "\\\""),
                '\\' => try output.appendSlice(allocator, "\\\\"),
                '\n' => try output.appendSlice(allocator, "\\n"),
                '\r' => try output.appendSlice(allocator, "\\r"),
                '\t' => try output.appendSlice(allocator, "\\t"),
                else => {
                    if (c < 0x20) {
                        var esc: [6]u8 = undefined;
                        _ = std.fmt.bufPrint(&esc, "\\u{x:0>4}", .{c}) catch unreachable;
                        try output.appendSlice(allocator, &esc);
                    } else {
                        try output.append(allocator, c);
                    }
                },
            }
        }
        try output.append(allocator, '"');
    } else {
        // Binary — base64 encode
        try output.append(allocator, '"');
        const encoded_len = std.base64.standard.Encoder.calcSize(data.len);
        const start = output.items.len;
        try output.resize(allocator, start + encoded_len);
        _ = std.base64.standard.Encoder.encode(output.items[start..], data);
        try output.append(allocator, '"');
    }
}

// =============================================================================
// Tests
// =============================================================================

test "readVarint basic" {
    {
        const data = [_]u8{0x08};
        var pos: usize = 0;
        try std.testing.expectEqual(@as(u64, 8), readVarint(&data, &pos).?);
    }
    {
        // 300 = 0xAC 0x02
        const data = [_]u8{ 0xAC, 0x02 };
        var pos: usize = 0;
        try std.testing.expectEqual(@as(u64, 300), readVarint(&data, &pos).?);
    }
}

test "decode simple protobuf message" {
    const allocator = std.testing.allocator;
    var decoder = ProtobufDecoder.init(allocator);

    // Field 1 = varint 150: tag = (1 << 3) | 0 = 0x08, value = 0x96 0x01
    const data = [_]u8{ 0x08, 0x96, 0x01 };
    const result = try decoder.decode(&data);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{\"1\":150}", result);
}

test "decode protobuf string field" {
    const allocator = std.testing.allocator;
    var decoder = ProtobufDecoder.init(allocator);

    // Field 2 = string "testing": tag = (2 << 3) | 2 = 0x12, len = 7
    const data = [_]u8{ 0x12, 0x07, 't', 'e', 's', 't', 'i', 'n', 'g' };
    const result = try decoder.decode(&data);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{\"2\":\"testing\"}", result);
}

test "decode protobuf multiple fields" {
    const allocator = std.testing.allocator;
    var decoder = ProtobufDecoder.init(allocator);

    // Field 1 = varint 42, Field 2 = string "hi"
    const data = [_]u8{
        0x08, 0x2A, // field 1, varint 42
        0x12, 0x02, 'h', 'i', // field 2, string "hi"
    };
    const result = try decoder.decode(&data);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{\"1\":42,\"2\":\"hi\"}", result);
}

test "decode protobuf fixed32" {
    const allocator = std.testing.allocator;
    var decoder = ProtobufDecoder.init(allocator);

    // Field 1 = fixed32 0x00000001: tag = (1 << 3) | 5 = 0x0D
    const data = [_]u8{ 0x0D, 0x01, 0x00, 0x00, 0x00 };
    const result = try decoder.decode(&data);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{\"1\":1}", result);
}

test "decode empty protobuf" {
    const allocator = std.testing.allocator;
    var decoder = ProtobufDecoder.init(allocator);
    const result = try decoder.decode(&.{});
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{}", result);
}

test "parseConfluentProtobufHeader" {
    const data = [_]u8{
        0x00, // magic
        0x00, 0x00, 0x00, 0x05, // schema_id = 5
        0x00, // message index count = 0
        0x08, 0x2A, // protobuf payload
    };
    const result = parseConfluentProtobufHeader(&data).?;
    try std.testing.expectEqualStrings(&[_]u8{ 0x08, 0x2A }, result.payload);
}

test "looksLikeProtobuf" {
    // Valid: field 1, varint
    try std.testing.expect(looksLikeProtobuf(&[_]u8{ 0x08, 0x01 }));
    // Valid: field 2, length-delimited
    try std.testing.expect(looksLikeProtobuf(&[_]u8{ 0x12, 0x02, 'h', 'i' }));
    // Invalid: empty
    try std.testing.expect(!looksLikeProtobuf(&[_]u8{}));
    // Invalid: field 0
    try std.testing.expect(!looksLikeProtobuf(&[_]u8{0x00}));
}
