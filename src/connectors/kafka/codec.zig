//! Kafka Wire Protocol Codec
//!
//! Encodes requests and decodes responses for the subset of Kafka APIs
//! needed by KafkaSource. All Kafka types are big-endian.
//!
//! Kafka primitive types:
//!   int8, int16, int32, int64 — big-endian signed integers
//!   varint/varlong — zigzag-encoded variable-length integers
//!   string — int16 length prefix + bytes (nullable: length = -1)
//!   compact_string — unsigned varint length + bytes (flexible versions)
//!   bytes — int32 length prefix + bytes (nullable: length = -1)
//!   compact_bytes — unsigned varint length + bytes
//!   array — int32 count + elements
//!   compact_array — unsigned varint count + elements
//!   tagged_fields — unsigned varint count + tag entries (flexible versions)

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Kafka API keys for the subset used by KafkaSource.
pub const ApiKey = enum(i16) {
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    SaslHandshake = 17,
    ApiVersions = 18,
    SaslAuthenticate = 36,

    pub fn toInt(self: ApiKey) i16 {
        return @intFromEnum(self);
    }
};

/// API version range reported by broker.
pub const ApiVersionRange = struct {
    min_version: i16 = 0,
    max_version: i16 = -1, // -1 = unsupported

    pub fn supports(self: ApiVersionRange, version: i16) bool {
        return self.max_version >= 0 and version >= self.min_version and version <= self.max_version;
    }

    pub fn negotiateMax(self: ApiVersionRange, our_max: i16) ?i16 {
        if (self.max_version < 0) return null;
        const version = @min(self.max_version, our_max);
        if (version < self.min_version) return null;
        return version;
    }
};

/// Flexible version boundaries per API.
/// Versions >= min_flex_version use compact encoding + tagged fields.
pub const FlexBoundary = struct {
    api_key: ApiKey,
    min_flex_version: i16,
};

pub const flex_boundaries = [_]FlexBoundary{
    .{ .api_key = .Fetch, .min_flex_version = 12 },
    .{ .api_key = .Metadata, .min_flex_version = 9 },
    .{ .api_key = .ListOffsets, .min_flex_version = 6 },
    .{ .api_key = .OffsetCommit, .min_flex_version = 8 },
    .{ .api_key = .OffsetFetch, .min_flex_version = 6 },
    .{ .api_key = .ApiVersions, .min_flex_version = 3 },
};

pub fn isFlexibleVersion(api_key: ApiKey, api_version: i16) bool {
    for (flex_boundaries) |fb| {
        if (fb.api_key == api_key) {
            return api_version >= fb.min_flex_version;
        }
    }
    return false;
}

// =============================================================================
// KafkaReader — Decodes Kafka binary protocol from a byte buffer
// =============================================================================

pub const KafkaReader = struct {
    data: []const u8,
    pos: usize,

    pub fn init(data: []const u8) KafkaReader {
        return .{ .data = data, .pos = 0 };
    }

    pub fn remaining(self: *const KafkaReader) usize {
        if (self.pos >= self.data.len) return 0;
        return self.data.len - self.pos;
    }

    /// Return the current read position.
    pub fn offset(self: *const KafkaReader) usize {
        return self.pos;
    }

    /// Return the underlying data slice.
    pub fn slice(self: *const KafkaReader) []const u8 {
        return self.data;
    }

    pub fn readInt8(self: *KafkaReader) !i8 {
        if (self.pos + 1 > self.data.len) return error.EndOfBuffer;
        const val: i8 = @bitCast(self.data[self.pos]);
        self.pos += 1;
        return val;
    }

    pub fn readUInt8(self: *KafkaReader) !u8 {
        if (self.pos + 1 > self.data.len) return error.EndOfBuffer;
        const val = self.data[self.pos];
        self.pos += 1;
        return val;
    }

    pub fn readInt16(self: *KafkaReader) !i16 {
        if (self.pos + 2 > self.data.len) return error.EndOfBuffer;
        const val = std.mem.readInt(i16, self.data[self.pos..][0..2], .big);
        self.pos += 2;
        return val;
    }

    pub fn readInt32(self: *KafkaReader) !i32 {
        if (self.pos + 4 > self.data.len) return error.EndOfBuffer;
        const val = std.mem.readInt(i32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    pub fn readUInt32(self: *KafkaReader) !u32 {
        if (self.pos + 4 > self.data.len) return error.EndOfBuffer;
        const val = std.mem.readInt(u32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    pub fn readInt64(self: *KafkaReader) !i64 {
        if (self.pos + 8 > self.data.len) return error.EndOfBuffer;
        const val = std.mem.readInt(i64, self.data[self.pos..][0..8], .big);
        self.pos += 8;
        return val;
    }

    /// Read a zigzag-encoded variable-length integer.
    pub fn readVarInt(self: *KafkaReader) !i32 {
        const raw = try self.readUnsignedVarInt();
        // zigzag decode: (n >>> 1) ^ -(n & 1)
        const n: i32 = @bitCast(raw);
        return (n >> 1) ^ (-(n & 1));
    }

    /// Read a zigzag-encoded variable-length long.
    pub fn readVarLong(self: *KafkaReader) !i64 {
        const raw = try self.readUnsignedVarLong();
        const n: i64 = @bitCast(raw);
        return (n >> 1) ^ (-(n & 1));
    }

    /// Read an unsigned variable-length integer (used for compact sizes).
    pub fn readUnsignedVarInt(self: *KafkaReader) !u32 {
        var result: u32 = 0;
        var shift: u5 = 0;
        while (true) {
            if (self.pos >= self.data.len) return error.EndOfBuffer;
            const b = self.data[self.pos];
            self.pos += 1;
            result |= @as(u32, b & 0x7F) << shift;
            if (b & 0x80 == 0) break;
            shift +%= 7;
            if (shift > 28) return error.VarIntTooLong;
        }
        return result;
    }

    /// Read an unsigned variable-length long.
    pub fn readUnsignedVarLong(self: *KafkaReader) !u64 {
        var result: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            if (self.pos >= self.data.len) return error.EndOfBuffer;
            const b = self.data[self.pos];
            self.pos += 1;
            result |= @as(u64, b & 0x7F) << shift;
            if (b & 0x80 == 0) break;
            shift +%= 7;
            if (shift > 56) return error.VarIntTooLong;
        }
        return result;
    }

    /// Read a Kafka string (int16 length prefix). Returns null for length = -1.
    pub fn readString(self: *KafkaReader) !?[]const u8 {
        const len = try self.readInt16();
        if (len < 0) return null;
        const ulen: usize = @intCast(len);
        if (self.pos + ulen > self.data.len) return error.EndOfBuffer;
        const result = self.data[self.pos..][0..ulen];
        self.pos += ulen;
        return result;
    }

    /// Read a compact string (unsigned varint length). Length 0 = null.
    pub fn readCompactString(self: *KafkaReader) !?[]const u8 {
        const raw_len = try self.readUnsignedVarInt();
        if (raw_len == 0) return null;
        const len: usize = @intCast(raw_len - 1); // length is N+1 encoded
        if (self.pos + len > self.data.len) return error.EndOfBuffer;
        const result = self.data[self.pos..][0..len];
        self.pos += len;
        return result;
    }

    /// Read Kafka bytes (int32 length prefix). Returns null for length = -1.
    pub fn readBytes(self: *KafkaReader) !?[]const u8 {
        const len = try self.readInt32();
        if (len < 0) return null;
        const ulen: usize = @intCast(len);
        if (self.pos + ulen > self.data.len) return error.EndOfBuffer;
        const result = self.data[self.pos..][0..ulen];
        self.pos += ulen;
        return result;
    }

    /// Read compact bytes (unsigned varint length). Length 0 = null.
    pub fn readCompactBytes(self: *KafkaReader) !?[]const u8 {
        const raw_len = try self.readUnsignedVarInt();
        if (raw_len == 0) return null;
        const len: usize = @intCast(raw_len - 1);
        if (self.pos + len > self.data.len) return error.EndOfBuffer;
        const result = self.data[self.pos..][0..len];
        self.pos += len;
        return result;
    }

    /// Read and skip tagged fields (flexible versions).
    pub fn readTaggedFields(self: *KafkaReader) !void {
        const count = try self.readUnsignedVarInt();
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            _ = try self.readUnsignedVarInt(); // tag
            const size = try self.readUnsignedVarInt(); // size
            if (self.pos + size > self.data.len) return error.EndOfBuffer;
            self.pos += @intCast(size);
        }
    }

    /// Read an int32 array count. Returns -1 for null arrays.
    pub fn readArrayLen(self: *KafkaReader) !i32 {
        return self.readInt32();
    }

    /// Read a compact array count (unsigned varint). Returns 0 for null (N+1 encoding).
    pub fn readCompactArrayLen(self: *KafkaReader) !u32 {
        const raw = try self.readUnsignedVarInt();
        if (raw == 0) return 0;
        return raw - 1;
    }

    /// Read raw bytes without a length prefix.
    pub fn readRawBytes(self: *KafkaReader, len: usize) ![]const u8 {
        if (self.pos + len > self.data.len) return error.EndOfBuffer;
        const result = self.data[self.pos..][0..len];
        self.pos += len;
        return result;
    }

    /// Skip n bytes.
    pub fn skip(self: *KafkaReader, n: usize) !void {
        if (self.pos + n > self.data.len) return error.EndOfBuffer;
        self.pos += n;
    }

    /// Get the current position.
    pub fn getPos(self: *const KafkaReader) usize {
        return self.pos;
    }
};

// =============================================================================
// KafkaWriter — Encodes Kafka binary protocol into a byte buffer
// =============================================================================

pub const KafkaWriter = struct {
    buf: std.ArrayList(u8),
    allocator: Allocator,

    pub fn init(allocator: Allocator) KafkaWriter {
        return .{
            .buf = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *KafkaWriter) void {
        self.buf.deinit(self.allocator);
    }

    pub fn toOwnedSlice(self: *KafkaWriter) ![]u8 {
        return self.buf.toOwnedSlice(self.allocator);
    }

    pub fn getWritten(self: *const KafkaWriter) []const u8 {
        return self.buf.items;
    }

    pub fn writeByte(self: *KafkaWriter, val: u8) !void {
        try self.buf.append(self.allocator, val);
    }

    pub fn writeInt16(self: *KafkaWriter, val: i16) !void {
        var bytes: [2]u8 = undefined;
        std.mem.writeInt(i16, &bytes, val, .big);
        try self.buf.appendSlice(self.allocator, &bytes);
    }

    pub fn writeInt32(self: *KafkaWriter, val: i32) !void {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(i32, &bytes, val, .big);
        try self.buf.appendSlice(self.allocator, &bytes);
    }

    pub fn writeUInt32(self: *KafkaWriter, val: u32) !void {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, val, .big);
        try self.buf.appendSlice(self.allocator, &bytes);
    }

    pub fn writeInt64(self: *KafkaWriter, val: i64) !void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &bytes, val, .big);
        try self.buf.appendSlice(self.allocator, &bytes);
    }

    /// Write a zigzag-encoded variable-length integer.
    pub fn writeVarInt(self: *KafkaWriter, val: i32) !void {
        // zigzag encode: (n << 1) ^ (n >> 31)
        const encoded: u32 = @bitCast((val << 1) ^ (val >> 31));
        try self.writeUnsignedVarInt(encoded);
    }

    /// Write an unsigned variable-length integer.
    pub fn writeUnsignedVarInt(self: *KafkaWriter, value: u32) !void {
        var val = value;
        while (val > 0x7F) {
            try self.buf.append(self.allocator, @as(u8, @truncate(val & 0x7F)) | 0x80);
            val >>= 7;
        }
        try self.buf.append(self.allocator, @truncate(val & 0x7F));
    }

    /// Write a Kafka string (int16 length prefix). Null = -1.
    pub fn writeString(self: *KafkaWriter, val: ?[]const u8) !void {
        if (val) |v| {
            try self.writeInt16(@intCast(v.len));
            try self.buf.appendSlice(self.allocator, v);
        } else {
            try self.writeInt16(-1);
        }
    }

    /// Write a compact string (unsigned varint N+1 length). Null = 0.
    pub fn writeCompactString(self: *KafkaWriter, val: ?[]const u8) !void {
        if (val) |v| {
            try self.writeUnsignedVarInt(@intCast(v.len + 1));
            try self.buf.appendSlice(self.allocator, v);
        } else {
            try self.writeUnsignedVarInt(0);
        }
    }

    /// Write Kafka bytes (int32 length prefix). Null = -1.
    pub fn writeBytes(self: *KafkaWriter, val: ?[]const u8) !void {
        if (val) |v| {
            try self.writeInt32(@intCast(v.len));
            try self.buf.appendSlice(self.allocator, v);
        } else {
            try self.writeInt32(-1);
        }
    }

    /// Write compact bytes (unsigned varint N+1 length). Null = 0.
    pub fn writeCompactBytes(self: *KafkaWriter, val: ?[]const u8) !void {
        if (val) |v| {
            try self.writeUnsignedVarInt(@intCast(v.len + 1));
            try self.buf.appendSlice(self.allocator, v);
        } else {
            try self.writeUnsignedVarInt(0);
        }
    }

    /// Write empty tagged fields section.
    pub fn writeTaggedFields(self: *KafkaWriter) !void {
        try self.writeUnsignedVarInt(0);
    }

    /// Write raw bytes (no length prefix).
    pub fn writeRawBytes(self: *KafkaWriter, val: []const u8) !void {
        try self.buf.appendSlice(self.allocator, val);
    }

    /// Returns the current length of the buffer.
    pub fn len(self: *const KafkaWriter) usize {
        return self.buf.items.len;
    }

    /// Overwrite 4 bytes at a given position (for backpatching length).
    pub fn patchInt32(self: *KafkaWriter, pos: usize, val: i32) void {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(i32, &bytes, val, .big);
        @memcpy(self.buf.items[pos..][0..4], &bytes);
    }
};

// =============================================================================
// Request/Response Framing
// =============================================================================

/// Decoded response header.
pub const ResponseFrame = struct {
    correlation_id: i32,
    body: []const u8,
};

/// Encode a complete Kafka request: [4-byte length][header][body].
pub fn encodeRequest(
    allocator: Allocator,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    client_id: []const u8,
    body: []const u8,
) ![]u8 {
    const is_flex = isFlexibleVersion(api_key, api_version);
    // Header size: api_key(2) + api_version(2) + correlation_id(4) + client_id_string
    // + tagged_fields if flexible
    const client_id_size: usize = if (is_flex) blk: {
        // compact string: varint(len+1) + bytes
        break :blk varIntSize(@intCast(client_id.len + 1)) + client_id.len;
    } else blk: {
        // regular string: int16 + bytes
        break :blk 2 + client_id.len;
    };
    const header_size: usize = 2 + 2 + 4 + client_id_size + if (is_flex) @as(usize, 1) else @as(usize, 0);
    const total_size = header_size + body.len;

    var buf = try allocator.alloc(u8, 4 + total_size);
    var pos: usize = 0;

    // Length prefix (does not include itself)
    std.mem.writeInt(i32, buf[pos..][0..4], @intCast(total_size), .big);
    pos += 4;

    // api_key
    std.mem.writeInt(i16, buf[pos..][0..2], api_key.toInt(), .big);
    pos += 2;

    // api_version
    std.mem.writeInt(i16, buf[pos..][0..2], api_version, .big);
    pos += 2;

    // correlation_id
    std.mem.writeInt(i32, buf[pos..][0..4], correlation_id, .big);
    pos += 4;

    // client_id
    if (is_flex) {
        pos += writeUnsignedVarIntBuf(buf[pos..], @intCast(client_id.len + 1));
        @memcpy(buf[pos..][0..client_id.len], client_id);
        pos += client_id.len;
        // tagged fields (empty)
        buf[pos] = 0;
        pos += 1;
    } else {
        std.mem.writeInt(i16, buf[pos..][0..2], @intCast(client_id.len), .big);
        pos += 2;
        @memcpy(buf[pos..][0..client_id.len], client_id);
        pos += client_id.len;
    }

    // Body
    @memcpy(buf[pos..][0..body.len], body);

    return buf;
}

/// Decode a response frame. Validates correlation_id matches expected.
pub fn decodeResponseHeader(data: []const u8, expected_correlation_id: i32) !ResponseFrame {
    if (data.len < 4) return error.EndOfBuffer;
    const correlation_id = std.mem.readInt(i32, data[0..4], .big);
    if (correlation_id != expected_correlation_id) return error.CorrelationIdMismatch;
    return .{
        .correlation_id = correlation_id,
        .body = data[4..],
    };
}

// =============================================================================
// Helpers
// =============================================================================

fn varIntSize(value: u32) usize {
    var val = value;
    var size: usize = 0;
    while (true) {
        size += 1;
        val >>= 7;
        if (val == 0) break;
    }
    return size;
}

fn writeUnsignedVarIntBuf(buf: []u8, value: u32) usize {
    var val = value;
    var pos: usize = 0;
    while (val > 0x7F) {
        buf[pos] = @as(u8, @truncate(val & 0x7F)) | 0x80;
        val >>= 7;
        pos += 1;
    }
    buf[pos] = @truncate(val & 0x7F);
    return pos + 1;
}

// =============================================================================
// Tests
// =============================================================================

test "KafkaReader reads int32 big-endian" {
    const data = [_]u8{ 0x00, 0x00, 0x01, 0x00 };
    var reader = KafkaReader.init(&data);
    try std.testing.expectEqual(@as(i32, 256), try reader.readInt32());
}

test "KafkaReader reads int16 big-endian" {
    const data = [_]u8{ 0x00, 0x0A };
    var reader = KafkaReader.init(&data);
    try std.testing.expectEqual(@as(i16, 10), try reader.readInt16());
}

test "KafkaReader reads int64 big-endian" {
    const data = [_]u8{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A };
    var reader = KafkaReader.init(&data);
    try std.testing.expectEqual(@as(i64, 42), try reader.readInt64());
}

test "KafkaReader reads varint positive" {
    // zigzag(42) = 84 = 0x54
    const data = [_]u8{0x54};
    var reader = KafkaReader.init(&data);
    try std.testing.expectEqual(@as(i32, 42), try reader.readVarInt());
}

test "KafkaReader reads varint negative" {
    // zigzag(-1) = 1 = 0x01
    const data = [_]u8{0x01};
    var reader = KafkaReader.init(&data);
    try std.testing.expectEqual(@as(i32, -1), try reader.readVarInt());
}

test "KafkaReader reads varint zero" {
    const data = [_]u8{0x00};
    var reader = KafkaReader.init(&data);
    try std.testing.expectEqual(@as(i32, 0), try reader.readVarInt());
}

test "KafkaReader reads string" {
    // length=5, "hello"
    const data = [_]u8{ 0x00, 0x05, 'h', 'e', 'l', 'l', 'o' };
    var reader = KafkaReader.init(&data);
    const str = try reader.readString();
    try std.testing.expectEqualSlices(u8, "hello", str.?);
}

test "KafkaReader reads null string" {
    // length=-1
    const data = [_]u8{ 0xFF, 0xFF };
    var reader = KafkaReader.init(&data);
    const str = try reader.readString();
    try std.testing.expect(str == null);
}

test "KafkaReader reads compact string" {
    // length = 6 (N+1 encoding, actual = 5), "hello"
    const data = [_]u8{ 0x06, 'h', 'e', 'l', 'l', 'o' };
    var reader = KafkaReader.init(&data);
    const str = try reader.readCompactString();
    try std.testing.expectEqualSlices(u8, "hello", str.?);
}

test "KafkaReader reads null compact string" {
    const data = [_]u8{0x00};
    var reader = KafkaReader.init(&data);
    const str = try reader.readCompactString();
    try std.testing.expect(str == null);
}

test "KafkaReader reads bytes" {
    const data = [_]u8{ 0x00, 0x00, 0x00, 0x03, 0xDE, 0xAD, 0xBE };
    var reader = KafkaReader.init(&data);
    const bytes = try reader.readBytes();
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xDE, 0xAD, 0xBE }, bytes.?);
}

test "KafkaReader reads null bytes" {
    const data = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF };
    var reader = KafkaReader.init(&data);
    const bytes = try reader.readBytes();
    try std.testing.expect(bytes == null);
}

test "KafkaWriter encodes int32 big-endian" {
    var writer = KafkaWriter.init(std.testing.allocator);
    defer writer.deinit();
    try writer.writeInt32(256);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x00, 0x01, 0x00 }, writer.getWritten());
}

test "KafkaWriter encodes string" {
    var writer = KafkaWriter.init(std.testing.allocator);
    defer writer.deinit();
    try writer.writeString("hi");
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x02, 'h', 'i' }, writer.getWritten());
}

test "KafkaWriter encodes null string" {
    var writer = KafkaWriter.init(std.testing.allocator);
    defer writer.deinit();
    try writer.writeString(null);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xFF, 0xFF }, writer.getWritten());
}

test "KafkaWriter encodes compact string" {
    var writer = KafkaWriter.init(std.testing.allocator);
    defer writer.deinit();
    try writer.writeCompactString("hi");
    // N+1 encoding: length=3, "hi"
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x03, 'h', 'i' }, writer.getWritten());
}

test "KafkaWriter encodes varint positive" {
    var writer = KafkaWriter.init(std.testing.allocator);
    defer writer.deinit();
    try writer.writeVarInt(42);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x54}, writer.getWritten());
}

test "KafkaWriter encodes varint negative" {
    var writer = KafkaWriter.init(std.testing.allocator);
    defer writer.deinit();
    try writer.writeVarInt(-1);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x01}, writer.getWritten());
}

test "encodeRequest non-flexible" {
    const body = [_]u8{ 0x01, 0x02 };
    const request = try encodeRequest(
        std.testing.allocator,
        .Metadata,
        1,
        1,
        "flo",
        &body,
    );
    defer std.testing.allocator.free(request);
    // Length prefix: 4 bytes
    // Header: api_key(2) + api_version(2) + correlation_id(4) + client_id_str(2+3) = 13
    // Body: 2
    // Total payload = 15
    const expected_len = std.mem.readInt(i32, request[0..4], .big);
    try std.testing.expectEqual(@as(i32, 15), expected_len);
    // api_key = 3 (Metadata)
    try std.testing.expectEqual(@as(i16, 3), std.mem.readInt(i16, request[4..6], .big));
    // api_version = 1
    try std.testing.expectEqual(@as(i16, 1), std.mem.readInt(i16, request[6..8], .big));
    // correlation_id = 1
    try std.testing.expectEqual(@as(i32, 1), std.mem.readInt(i32, request[8..12], .big));
}

test "decodeResponseHeader" {
    const data = [_]u8{ 0x00, 0x00, 0x00, 0x05, 0xDE, 0xAD };
    const frame = try decodeResponseHeader(&data, 5);
    try std.testing.expectEqual(@as(i32, 5), frame.correlation_id);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xDE, 0xAD }, frame.body);
}

test "decodeResponseHeader bad correlation id" {
    const data = [_]u8{ 0x00, 0x00, 0x00, 0x05, 0xDE, 0xAD };
    try std.testing.expectError(error.CorrelationIdMismatch, decodeResponseHeader(&data, 99));
}

test "isFlexibleVersion" {
    try std.testing.expect(isFlexibleVersion(.Fetch, 12));
    try std.testing.expect(!isFlexibleVersion(.Fetch, 11));
    try std.testing.expect(isFlexibleVersion(.ApiVersions, 3));
    try std.testing.expect(!isFlexibleVersion(.ApiVersions, 2));
}

test "KafkaReader reads tagged fields" {
    // 2 tagged fields: tag=0 size=2 data=0xAA,0xBB; tag=1 size=1 data=0xCC
    const data = [_]u8{ 0x02, 0x00, 0x02, 0xAA, 0xBB, 0x01, 0x01, 0xCC };
    var reader = KafkaReader.init(&data);
    try reader.readTaggedFields();
    try std.testing.expectEqual(@as(usize, 8), reader.pos);
}

test "KafkaReader multi-byte varint" {
    // 300 zigzag = 600. 600 = 0b1001011000 => varint bytes: 0xD8, 0x04
    const data = [_]u8{ 0xD8, 0x04 };
    var reader = KafkaReader.init(&data);
    try std.testing.expectEqual(@as(i32, 300), try reader.readVarInt());
}
