//! Wire Protocol Utilities
//!
//! Safe, alignment-aware helpers for reading and writing binary data from/to
//! network buffers. These utilities handle endianness and prevent alignment
//! panics that occur when casting raw byte slices to structured types.
//!
//! Usage:
//!   const reader = WireReader.init(buffer);
//!   const id = reader.readU64() orelse return error.InvalidPayload;
//!   const name = reader.readLengthPrefixed(u16) orelse return error.InvalidPayload;
//!
//!   var writer = WireWriter.init(allocator);
//!   defer writer.deinit();
//!   try writer.writeU64(id);
//!   try writer.writeLengthPrefixed(u16, name);
//!   const bytes = try writer.toOwnedSlice();

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Safe reader for unaligned network byte buffers.
/// All reads are little-endian and alignment-safe.
pub const WireReader = struct {
    data: []const u8,
    pos: usize,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{ .data = data, .pos = 0 };
    }

    /// Returns remaining unread bytes
    pub fn remaining(self: Self) []const u8 {
        return if (self.pos < self.data.len) self.data[self.pos..] else &[_]u8{};
    }

    /// Returns number of bytes remaining
    pub fn remainingLen(self: Self) usize {
        return if (self.pos < self.data.len) self.data.len - self.pos else 0;
    }

    /// Check if there are at least `n` bytes remaining
    pub fn hasAtLeast(self: Self, n: usize) bool {
        return self.remainingLen() >= n;
    }

    /// Read a single byte
    pub fn readU8(self: *Self) ?u8 {
        if (self.pos >= self.data.len) return null;
        const val = self.data[self.pos];
        self.pos += 1;
        return val;
    }

    /// Read a u16 (little-endian, alignment-safe)
    pub fn readU16(self: *Self) ?u16 {
        if (self.pos + 2 > self.data.len) return null;
        const val = std.mem.readInt(u16, self.data[self.pos..][0..2], .little);
        self.pos += 2;
        return val;
    }

    /// Read a u32 (little-endian, alignment-safe)
    pub fn readU32(self: *Self) ?u32 {
        if (self.pos + 4 > self.data.len) return null;
        const val = std.mem.readInt(u32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        return val;
    }

    /// Read a u64 (little-endian, alignment-safe)
    pub fn readU64(self: *Self) ?u64 {
        if (self.pos + 8 > self.data.len) return null;
        const val = std.mem.readInt(u64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return val;
    }

    /// Read an i64 (little-endian, alignment-safe)
    pub fn readI64(self: *Self) ?i64 {
        if (self.pos + 8 > self.data.len) return null;
        const val = std.mem.readInt(i64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return val;
    }

    /// Read a fixed-size byte array
    pub fn readBytes(self: *Self, comptime n: usize) ?*const [n]u8 {
        if (self.pos + n > self.data.len) return null;
        const ptr = self.data[self.pos..][0..n];
        self.pos += n;
        return ptr;
    }

    /// Read a variable-length slice (no length prefix, just raw bytes)
    pub fn readSlice(self: *Self, len: usize) ?[]const u8 {
        if (self.pos + len > self.data.len) return null;
        const slice = self.data[self.pos..][0..len];
        self.pos += len;
        return slice;
    }

    /// Read a length-prefixed slice where the length is stored as type T (u8, u16, u32)
    pub fn readLengthPrefixed(self: *Self, comptime T: type) ?[]const u8 {
        const len = switch (T) {
            u8 => self.readU8(),
            u16 => self.readU16(),
            u32 => self.readU32(),
            else => @compileError("Unsupported length type"),
        } orelse return null;

        return self.readSlice(len);
    }

    /// Read an array of u64 values (alignment-safe, reads byte-by-byte)
    /// Returns a slice backed by the provided allocator
    pub fn readU64Array(self: *Self, count: usize, allocator: Allocator) ![]u64 {
        const byte_len = count * 8;
        if (self.pos + byte_len > self.data.len) return error.UnexpectedEndOfData;

        const result = try allocator.alloc(u64, count);
        errdefer allocator.free(result);

        for (0..count) |i| {
            result[i] = std.mem.readInt(u64, self.data[self.pos..][0..8], .little);
            self.pos += 8;
        }

        return result;
    }

    /// Skip n bytes
    pub fn skip(self: *Self, n: usize) bool {
        if (self.pos + n > self.data.len) return false;
        self.pos += n;
        return true;
    }

    /// Read a pair of length-prefixed slices (key/value, group/consumer, name/payload, etc.)
    /// Format: [key_len:KLen][key][value_len:VLen][value]
    pub fn readPair(
        self: *Self,
        comptime KLen: type,
        comptime VLen: type,
    ) ?struct { key: []const u8, value: []const u8 } {
        const key = self.readLengthPrefixed(KLen) orelse return null;
        const value = self.readLengthPrefixed(VLen) orelse return null;
        return .{ .key = key, .value = value };
    }
};

/// Writer for building binary wire protocol messages.
/// All writes are little-endian.
pub const WireWriter = struct {
    buffer: std.ArrayListUnmanaged(u8),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{ .buffer = .empty, .allocator = allocator };
    }

    pub fn initCapacity(allocator: Allocator, capacity: usize) !Self {
        var buffer: std.ArrayListUnmanaged(u8) = .empty;
        try buffer.ensureTotalCapacity(allocator, capacity);
        return .{ .buffer = buffer, .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        self.buffer.deinit(self.allocator);
    }

    /// Get the written bytes (borrowed, valid until next write or deinit)
    pub fn bytes(self: Self) []const u8 {
        return self.buffer.items;
    }

    /// Get the written bytes and transfer ownership to caller
    pub fn toOwnedSlice(self: *Self) ![]u8 {
        return self.buffer.toOwnedSlice(self.allocator);
    }

    /// Write a single byte
    /// Format: [val:u8]
    pub fn writeU8(self: *Self, val: u8) !void {
        try self.buffer.append(self.allocator, val);
    }

    /// Write a u16 (little-endian)
    /// Format: [val:u16]
    pub fn writeU16(self: *Self, val: u16) !void {
        var buf: [2]u8 = undefined;
        std.mem.writeInt(u16, &buf, val, .little);
        try self.buffer.appendSlice(self.allocator, &buf);
    }

    /// Write a u32 (little-endian)
    /// Format: [val:u32]
    pub fn writeU32(self: *Self, val: u32) !void {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, val, .little);
        try self.buffer.appendSlice(self.allocator, &buf);
    }

    /// Write a u64 (little-endian)
    /// Format: [val:u64]
    pub fn writeU64(self: *Self, val: u64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, val, .little);
        try self.buffer.appendSlice(self.allocator, &buf);
    }

    /// Write an i64 (little-endian)
    /// Format: [val:i64]
    pub fn writeI64(self: *Self, val: i64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, val, .little);
        try self.buffer.appendSlice(self.allocator, &buf);
    }

    /// Write raw bytes (no length prefix)
    /// Format: [data...]
    pub fn writeSlice(self: *Self, data: []const u8) !void {
        try self.buffer.appendSlice(self.allocator, data);
    }

    /// Write a length-prefixed slice where the length is stored as type T
    /// Format: [len:T][data...] where T is u8, u16, or u32
    pub fn writeLengthPrefixed(self: *Self, comptime T: type, data: []const u8) !void {
        const len: T = @intCast(data.len);
        switch (T) {
            u8 => try self.writeU8(len),
            u16 => try self.writeU16(len),
            u32 => try self.writeU32(len),
            else => @compileError("Unsupported length type"),
        }
        try self.writeSlice(data);
    }

    /// Write a pair of length-prefixed slices (mirrors readPair)
    /// Format: [key_len:KLen][key][value_len:VLen][value]
    pub fn writePair(
        self: *Self,
        comptime KLen: type,
        comptime VLen: type,
        key: []const u8,
        value: []const u8,
    ) !void {
        try self.writeLengthPrefixed(KLen, key);
        try self.writeLengthPrefixed(VLen, value);
    }

    /// Write an array of u64 values (no count prefix)
    /// Format: ([val:u64])*
    pub fn writeU64Array(self: *Self, values: []const u64) !void {
        for (values) |val| {
            try self.writeU64(val);
        }
    }

    /// Write an array of u32 values (no count prefix)
    /// Format: ([val:u32])*
    pub fn writeU32Array(self: *Self, values: []const u32) !void {
        for (values) |val| {
            try self.writeU32(val);
        }
    }

    /// Write an array with a count prefix, using a custom write function for each element
    /// Format: [count:u32]([element])*
    pub fn writeArray(
        self: *Self,
        items: anytype,
        writeFn: anytype,
    ) !void {
        try self.writeU32(@intCast(items.len));
        for (items) |item| {
            try writeFn(self, item);
        }
    }

    /// Write an array with a count prefix, passing extra context to the write function
    /// Format: [count:u32]([element])*
    pub fn writeArrayCtx(
        self: *Self,
        items: anytype,
        ctx: anytype,
        writeFn: anytype,
    ) !void {
        try self.writeU32(@intCast(items.len));
        for (items) |item| {
            try writeFn(self, item, ctx);
        }
    }

    /// Write a single header (key-value pair with u16 length prefixes)
    /// Format: [key_len:u16][key][value_len:u16][value]
    pub fn writeHeader(self: *Self, header: anytype) !void {
        try self.writePair(u16, u16, header.key, header.value);
    }

    /// Write an array of headers with a u16 count prefix
    /// Format: [count:u16][header]*
    pub fn writeHeaders(self: *Self, headers: anytype) !void {
        try self.writeU16(@intCast(headers.len));
        for (headers) |header| {
            try self.writeHeader(header);
        }
    }
};

/// Fixed-buffer writer for building wire protocol messages without allocation.
/// Use when you have a stack-allocated buffer and know the max size.
/// All writes are little-endian.
pub fn FixedWireWriter(comptime capacity: usize) type {
    return struct {
        buffer: [capacity]u8 = undefined,
        pos: usize = 0,

        const Self = @This();

        pub fn init() Self {
            return .{};
        }

        /// Get the written bytes
        pub fn bytes(self: *const Self) []const u8 {
            return self.buffer[0..self.pos];
        }

        /// Current length
        pub fn len(self: Self) usize {
            return self.pos;
        }

        /// Remaining capacity
        pub fn remainingCapacity(self: Self) usize {
            return capacity - self.pos;
        }

        /// Write a single byte
        pub fn writeU8(self: *Self, val: u8) !void {
            if (self.pos + 1 > capacity) return error.BufferOverflow;
            self.buffer[self.pos] = val;
            self.pos += 1;
        }

        /// Write a u16 (little-endian)
        pub fn writeU16(self: *Self, val: u16) !void {
            if (self.pos + 2 > capacity) return error.BufferOverflow;
            std.mem.writeInt(u16, self.buffer[self.pos..][0..2], val, .little);
            self.pos += 2;
        }

        /// Write a u32 (little-endian)
        pub fn writeU32(self: *Self, val: u32) !void {
            if (self.pos + 4 > capacity) return error.BufferOverflow;
            std.mem.writeInt(u32, self.buffer[self.pos..][0..4], val, .little);
            self.pos += 4;
        }

        /// Write a u64 (little-endian)
        pub fn writeU64(self: *Self, val: u64) !void {
            if (self.pos + 8 > capacity) return error.BufferOverflow;
            std.mem.writeInt(u64, self.buffer[self.pos..][0..8], val, .little);
            self.pos += 8;
        }

        /// Write an i64 (little-endian)
        pub fn writeI64(self: *Self, val: i64) !void {
            if (self.pos + 8 > capacity) return error.BufferOverflow;
            std.mem.writeInt(i64, self.buffer[self.pos..][0..8], val, .little);
            self.pos += 8;
        }

        /// Write raw bytes
        pub fn writeSlice(self: *Self, data: []const u8) !void {
            if (self.pos + data.len > capacity) return error.BufferOverflow;
            @memcpy(self.buffer[self.pos..][0..data.len], data);
            self.pos += data.len;
        }

        /// Write a length-prefixed slice
        pub fn writeLengthPrefixed(self: *Self, comptime T: type, data: []const u8) !void {
            const len_val: T = @intCast(data.len);
            switch (T) {
                u8 => try self.writeU8(len_val),
                u16 => try self.writeU16(len_val),
                u32 => try self.writeU32(len_val),
                else => @compileError("Unsupported length type"),
            }
            try self.writeSlice(data);
        }

        /// Write a key-value pair
        pub fn writePair(
            self: *Self,
            comptime KLen: type,
            comptime VLen: type,
            key: []const u8,
            value: []const u8,
        ) !void {
            try self.writeLengthPrefixed(KLen, key);
            try self.writeLengthPrefixed(VLen, value);
        }

        /// Write an array of u64 values (no count prefix)
        pub fn writeU64Array(self: *Self, values: []const u64) !void {
            for (values) |val| {
                try self.writeU64(val);
            }
        }

        /// Write an array of u64 values with a u32 count prefix
        pub fn writeU64ArrayWithCount(self: *Self, values: []const u64) !void {
            try self.writeU32(@intCast(values.len));
            try self.writeU64Array(values);
        }
    };
}

// =============================================================================
// Tests
// =============================================================================

test "WireReader: basic integer reads" {
    const data = [_]u8{
        0x01, // u8 = 1
        0x02, 0x00, // u16 = 2
        0x03, 0x00, 0x00, 0x00, // u32 = 3
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // u64 = 4
    };

    var reader = WireReader.init(&data);

    try std.testing.expectEqual(@as(u8, 1), reader.readU8().?);
    try std.testing.expectEqual(@as(u16, 2), reader.readU16().?);
    try std.testing.expectEqual(@as(u32, 3), reader.readU32().?);
    try std.testing.expectEqual(@as(u64, 4), reader.readU64().?);
    try std.testing.expectEqual(@as(usize, 0), reader.remainingLen());
}

test "WireReader: length-prefixed strings" {
    const data = [_]u8{
        0x05, 0x00, // u16 len = 5
        'h',  'e',
        'l',  'l',
        'o',
        0x03, // u8 len = 3
        'f',
        'o',
        'o',
    };

    var reader = WireReader.init(&data);

    const str1 = reader.readLengthPrefixed(u16).?;
    try std.testing.expectEqualStrings("hello", str1);

    const str2 = reader.readLengthPrefixed(u8).?;
    try std.testing.expectEqualStrings("foo", str2);
}

test "WireReader: returns null on insufficient data" {
    const data = [_]u8{ 0x01, 0x02 };
    var reader = WireReader.init(&data);

    try std.testing.expect(reader.readU16() != null);
    try std.testing.expect(reader.readU16() == null); // Not enough bytes
}

test "WireReader: readU64Array alignment-safe" {
    // Deliberately misalign by prepending a byte
    const data = [_]u8{
        0xFF, // padding byte (skip this)
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // u64 = 1
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // u64 = 2
    };

    var reader = WireReader.init(data[1..]); // Start at offset 1 (unaligned)
    const arr = try reader.readU64Array(2, std.testing.allocator);
    defer std.testing.allocator.free(arr);

    try std.testing.expectEqual(@as(u64, 1), arr[0]);
    try std.testing.expectEqual(@as(u64, 2), arr[1]);
}

test "WireWriter: basic writes" {
    var writer = WireWriter.init(std.testing.allocator);
    defer writer.deinit();

    try writer.writeU8(1);
    try writer.writeU16(2);
    try writer.writeU32(3);
    try writer.writeU64(4);

    const expected = [_]u8{
        0x01, // u8 = 1
        0x02, 0x00, // u16 = 2
        0x03, 0x00, 0x00, 0x00, // u32 = 3
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // u64 = 4
    };

    try std.testing.expectEqualSlices(u8, &expected, writer.bytes());
}

test "WireWriter: length-prefixed writes" {
    var writer = WireWriter.init(std.testing.allocator);
    defer writer.deinit();

    try writer.writeLengthPrefixed(u16, "hello");
    try writer.writeLengthPrefixed(u8, "foo");

    const expected = [_]u8{
        0x05, 0x00, // u16 len = 5
        'h',  'e',
        'l',  'l',
        'o',
        0x03, // u8 len = 3
        'f',
        'o',
        'o',
    };

    try std.testing.expectEqualSlices(u8, &expected, writer.bytes());
}

test "WireWriter and WireReader roundtrip" {
    const allocator = std.testing.allocator;

    // Write
    var writer = WireWriter.init(allocator);
    defer writer.deinit();

    try writer.writeU32(42);
    try writer.writeLengthPrefixed(u16, "test_key");
    try writer.writeI64(-12345);

    // Read back
    var reader = WireReader.init(writer.bytes());

    try std.testing.expectEqual(@as(u32, 42), reader.readU32().?);
    try std.testing.expectEqualStrings("test_key", reader.readLengthPrefixed(u16).?);
    try std.testing.expectEqual(@as(i64, -12345), reader.readI64().?);
}

test "WireWriter: writeArray with custom serializer" {
    const allocator = std.testing.allocator;

    // Example: writing a simple struct array
    const Item = struct { id: u32, name: []const u8 };
    const items = [_]Item{
        .{ .id = 1, .name = "foo" },
        .{ .id = 2, .name = "bar" },
    };

    var writer = WireWriter.init(allocator);
    defer writer.deinit();

    // Use writeArray with inline function
    try writer.writeArray(&items, struct {
        fn write(w: *WireWriter, item: Item) !void {
            try w.writeU32(item.id);
            try w.writeLengthPrefixed(u8, item.name);
        }
    }.write);

    // Verify: [count:u32][id:u32][name_len:u8][name]...
    var reader = WireReader.init(writer.bytes());
    try std.testing.expectEqual(@as(u32, 2), reader.readU32().?); // count
    try std.testing.expectEqual(@as(u32, 1), reader.readU32().?); // item 0 id
    try std.testing.expectEqualStrings("foo", reader.readLengthPrefixed(u8).?);
    try std.testing.expectEqual(@as(u32, 2), reader.readU32().?); // item 1 id
    try std.testing.expectEqualStrings("bar", reader.readLengthPrefixed(u8).?);
}

test "WireWriter: writeArrayCtx with context" {
    const allocator = std.testing.allocator;

    const Entry = struct { key: []const u8, value: []const u8, version: u64 };
    const entries = [_]Entry{
        .{ .key = "k1", .value = "v1", .version = 100 },
    };

    var writer = WireWriter.init(allocator);
    defer writer.deinit();

    const keys_only = true;
    try writer.writeArrayCtx(&entries, keys_only, struct {
        fn write(w: *WireWriter, entry: Entry, skip_version: bool) !void {
            try w.writeLengthPrefixed(u16, entry.key);
            try w.writeLengthPrefixed(u32, entry.value);
            if (!skip_version) {
                try w.writeU64(entry.version);
            }
        }
    }.write);

    // Verify: version should be omitted
    var reader = WireReader.init(writer.bytes());
    try std.testing.expectEqual(@as(u32, 1), reader.readU32().?); // count
    try std.testing.expectEqualStrings("k1", reader.readLengthPrefixed(u16).?);
    try std.testing.expectEqualStrings("v1", reader.readLengthPrefixed(u32).?);
    try std.testing.expectEqual(@as(usize, 0), reader.remainingLen()); // no version
}
