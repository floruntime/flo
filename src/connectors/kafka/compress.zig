//! Kafka Compression Codecs
//!
//! Implements decompression for Snappy, LZ4, and ZSTD — the three codecs
//! used by Kafka RecordBatch (in addition to GZIP, handled elsewhere).

const std = @import("std");
const Allocator = std.mem.Allocator;

const log = @import("stdx").log;

// =============================================================================
// Snappy Decompression
// =============================================================================
//
// Kafka uses raw Snappy format (NOT framing format):
//   - Varint: uncompressed length
//   - Elements: literal or copy commands
//
// Element types (tag byte low 2 bits):
//   00 = literal
//   01 = copy with 1-byte offset
//   10 = copy with 2-byte offset
//   11 = copy with 4-byte offset

pub fn decompressSnappy(data: []const u8, allocator: Allocator) ![]u8 {
    if (data.len == 0) return error.DecompressionFailed;

    var pos: usize = 0;

    // Read uncompressed length (varint)
    const uncompressed_len = readSnappyVarint(data, &pos) orelse return error.DecompressionFailed;
    if (uncompressed_len > 128 * 1024 * 1024) return error.DecompressionFailed; // 128MB sanity limit

    const output = try allocator.alloc(u8, uncompressed_len);
    errdefer allocator.free(output);
    var out_pos: usize = 0;

    while (pos < data.len and out_pos < uncompressed_len) {
        const tag = data[pos];
        pos += 1;

        const element_type: u2 = @intCast(tag & 0x03);

        switch (element_type) {
            0x00 => {
                // Literal
                const literal_len = blk: {
                    const len_minus_one = tag >> 2;
                    if (len_minus_one < 60) {
                        break :blk @as(usize, len_minus_one) + 1;
                    }
                    // Extended literal length
                    const extra_bytes: usize = @as(usize, len_minus_one) - 59;
                    if (pos + extra_bytes > data.len) return error.DecompressionFailed;
                    var length: usize = 0;
                    for (0..extra_bytes) |i| {
                        length |= @as(usize, data[pos + i]) << @intCast(i * 8);
                    }
                    pos += extra_bytes;
                    break :blk length + 1;
                };

                if (pos + literal_len > data.len) return error.DecompressionFailed;
                if (out_pos + literal_len > uncompressed_len) return error.DecompressionFailed;

                @memcpy(output[out_pos..][0..literal_len], data[pos..][0..literal_len]);
                pos += literal_len;
                out_pos += literal_len;
            },
            0x01 => {
                // Copy with 1-byte offset (length 4-11, offset 0-2047)
                if (pos >= data.len) return error.DecompressionFailed;
                const length: usize = @as(usize, (tag >> 2) & 0x07) + 4;
                const offset: usize = (@as(usize, tag >> 5) << 8) | @as(usize, data[pos]);
                pos += 1;

                if (offset == 0 or offset > out_pos) return error.DecompressionFailed;
                if (out_pos + length > uncompressed_len) return error.DecompressionFailed;

                copyOverlapping(output, out_pos, offset, length);
                out_pos += length;
            },
            0x02 => {
                // Copy with 2-byte offset (length 1-64)
                if (pos + 2 > data.len) return error.DecompressionFailed;
                const length: usize = @as(usize, tag >> 2) + 1;
                const offset: usize = @as(usize, data[pos]) | (@as(usize, data[pos + 1]) << 8);
                pos += 2;

                if (offset == 0 or offset > out_pos) return error.DecompressionFailed;
                if (out_pos + length > uncompressed_len) return error.DecompressionFailed;

                copyOverlapping(output, out_pos, offset, length);
                out_pos += length;
            },
            0x03 => {
                // Copy with 4-byte offset (length 1-64)
                if (pos + 4 > data.len) return error.DecompressionFailed;
                const length: usize = @as(usize, tag >> 2) + 1;
                const offset: usize = @as(usize, data[pos]) |
                    (@as(usize, data[pos + 1]) << 8) |
                    (@as(usize, data[pos + 2]) << 16) |
                    (@as(usize, data[pos + 3]) << 24);
                pos += 4;

                if (offset == 0 or offset > out_pos) return error.DecompressionFailed;
                if (out_pos + length > uncompressed_len) return error.DecompressionFailed;

                copyOverlapping(output, out_pos, offset, length);
                out_pos += length;
            },
        }
    }

    if (out_pos != uncompressed_len) return error.DecompressionFailed;
    return output;
}

/// Copy bytes from an earlier position in the output buffer, handling overlap.
fn copyOverlapping(output: []u8, out_pos: usize, offset: usize, length: usize) void {
    const src_start = out_pos - offset;
    // Byte-by-byte copy handles overlapping regions correctly
    for (0..length) |i| {
        output[out_pos + i] = output[src_start + (i % offset)];
    }
}

/// Read a Snappy-style varint (little-endian, unsigned, 7 bits per byte).
fn readSnappyVarint(data: []const u8, pos: *usize) ?usize {
    var result: usize = 0;
    var shift: u6 = 0;
    while (pos.* < data.len) {
        const byte = data[pos.*];
        pos.* += 1;
        result |= @as(usize, byte & 0x7F) << shift;
        if (byte & 0x80 == 0) return result;
        shift +|= 7;
        if (shift >= 64) return null;
    }
    return null;
}

// =============================================================================
// LZ4 Decompression (LZ4 Frame Format — KIP-57)
// =============================================================================
//
// Kafka uses the LZ4 frame format (magic 0x184D2204):
//   Frame Header: magic(4) + FLG(1) + BD(1) + [ContentSize(8)] + [DictID(4)] + HC(1)
//   Blocks: blockSize(4) + blockData(blockSize & 0x7FFFFFFF)
//     - If high bit set: block is uncompressed
//     - If blockSize == 0: end mark
//   [ContentChecksum(4)]
//
// LZ4 block format:
//   Sequence: token(1) + [extraLiteralLen] + literals + offset(2) + [extraMatchLen]
//   Token high nibble = literal length (0-14, 15=extended)
//   Token low nibble = match length - 4 (0-14, 15=extended)

pub fn decompressLz4(data: []const u8, allocator: Allocator) ![]u8 {
    if (data.len < 7) return error.DecompressionFailed;

    // Verify LZ4 frame magic
    const magic = std.mem.readInt(u32, data[0..4], .little);
    if (magic != 0x184D2204) return error.DecompressionFailed;

    var pos: usize = 4;

    // Frame descriptor: FLG byte
    if (pos >= data.len) return error.DecompressionFailed;
    const flg = data[pos];
    pos += 1;

    const has_content_size = (flg & 0x08) != 0;
    const has_dict_id = (flg & 0x01) != 0;
    const version = (flg >> 6) & 0x03;
    if (version != 1) return error.DecompressionFailed;

    // BD byte (block max size descriptor)
    if (pos >= data.len) return error.DecompressionFailed;
    // const bd = data[pos]; // block_max_size encoded here, we don't need it
    pos += 1;

    // Optional content size (8 bytes LE)
    var content_size: ?u64 = null;
    if (has_content_size) {
        if (pos + 8 > data.len) return error.DecompressionFailed;
        content_size = std.mem.readInt(u64, data[pos..][0..8], .little);
        pos += 8;
    }

    // Optional dictionary ID (4 bytes)
    if (has_dict_id) {
        if (pos + 4 > data.len) return error.DecompressionFailed;
        pos += 4;
    }

    // Header checksum (1 byte)
    if (pos >= data.len) return error.DecompressionFailed;
    pos += 1; // skip HC

    // Decompress blocks
    var output: std.ArrayList(u8) = .{};
    errdefer output.deinit(allocator);

    // Pre-allocate if content size is known
    if (content_size) |cs| {
        if (cs <= 128 * 1024 * 1024) {
            try output.ensureTotalCapacity(allocator, @intCast(cs));
        }
    }

    while (pos + 4 <= data.len) {
        const block_header = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;

        if (block_header == 0) break; // end mark

        const is_uncompressed = (block_header & 0x80000000) != 0;
        const block_size: usize = @intCast(block_header & 0x7FFFFFFF);

        if (pos + block_size > data.len) return error.DecompressionFailed;
        if (block_size > 4 * 1024 * 1024) return error.DecompressionFailed; // 4MB block limit

        const block_data = data[pos..][0..block_size];
        pos += block_size;

        if (is_uncompressed) {
            try output.appendSlice(allocator, block_data);
        } else {
            try decompressLz4Block(block_data, &output, allocator);
        }
    }

    return output.toOwnedSlice(allocator);
}

/// Decompress a single LZ4 block into the output buffer.
fn decompressLz4Block(block: []const u8, output: *std.ArrayList(u8), allocator: Allocator) !void {
    var bpos: usize = 0;

    while (bpos < block.len) {
        const token = block[bpos];
        bpos += 1;

        // Literal length
        var literal_len: usize = @as(usize, token >> 4);
        if (literal_len == 15) {
            while (bpos < block.len) {
                const extra = block[bpos];
                bpos += 1;
                literal_len += extra;
                if (extra != 255) break;
            }
        }

        // Copy literals
        if (literal_len > 0) {
            if (bpos + literal_len > block.len) return error.DecompressionFailed;
            try output.appendSlice(allocator, block[bpos..][0..literal_len]);
            bpos += literal_len;
        }

        // Check if this was the last sequence (no match after last literal)
        if (bpos >= block.len) break;

        // Match offset (2 bytes, little-endian)
        if (bpos + 2 > block.len) return error.DecompressionFailed;
        const offset: usize = @as(usize, block[bpos]) | (@as(usize, block[bpos + 1]) << 8);
        bpos += 2;

        if (offset == 0) return error.DecompressionFailed;
        if (offset > output.items.len) return error.DecompressionFailed;

        // Match length
        var match_len: usize = @as(usize, token & 0x0F) + 4; // min match = 4
        if ((token & 0x0F) == 15) {
            while (bpos < block.len) {
                const extra = block[bpos];
                bpos += 1;
                match_len += extra;
                if (extra != 255) break;
            }
        }

        // Copy match (may overlap)
        const match_start = output.items.len - offset;
        try output.ensureUnusedCapacity(allocator, match_len);
        for (0..match_len) |i| {
            output.appendAssumeCapacity(output.items[match_start + (i % offset)]);
        }
    }
}

// =============================================================================
// ZSTD Decompression (via std.compress.zstd)
// =============================================================================

pub fn decompressZstd(data: []const u8, allocator: Allocator) ![]u8 {
    if (data.len < 4) return error.DecompressionFailed;

    var input_reader: std.Io.Reader = .fixed(data);
    var decompress_buf: [std.compress.zstd.default_window_len + std.compress.zstd.block_size_max]u8 = undefined;
    var decomp = std.compress.zstd.Decompress.init(&input_reader, &decompress_buf, .{});

    var result: std.ArrayList(u8) = .{};
    errdefer result.deinit(allocator);

    var buf: [8192]u8 = undefined;
    while (true) {
        const n = decomp.reader.readSliceShort(&buf) catch |err| {
            log.err("ZSTD decompression failed: {}", .{err});
            return error.DecompressionFailed;
        };
        if (n == 0) break;
        try result.appendSlice(allocator, buf[0..n]);
    }

    return result.toOwnedSlice(allocator);
}

// =============================================================================
// Tests
// =============================================================================

test "Snappy: empty input fails" {
    try std.testing.expectError(error.DecompressionFailed, decompressSnappy(&[_]u8{}, std.testing.allocator));
}

test "Snappy: literal-only roundtrip" {
    // Encode "hello" manually in raw snappy format:
    // varint(5) = 0x05, then literal tag for 5 bytes: ((5-1) << 2) | 0 = 0x10
    const compressed = [_]u8{
        0x05, // uncompressed length = 5
        0x10, // literal, length = (4 << 2) | 0 = 16 → (tag >> 2) = 4, length = 4+1 = 5
        'h',
        'e',
        'l',
        'l',
        'o',
    };
    const result = try decompressSnappy(&compressed, std.testing.allocator);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("hello", result);
}

test "Snappy: literal + copy" {
    // "abcabc" = literal "abc" + copy(offset=3, length=3)
    // but copy-1 min length is 4, so use copy-2 for length 3 → ((3-1) << 2) | 0x02 = 0x0A
    // Actually copy-2 length = (tag >> 2) + 1, so for length 3: (2 << 2) | 0x02 = 0x0A
    // offset = 3 in little-endian 2 bytes
    const compressed = [_]u8{
        0x06, // uncompressed length = 6
        0x08, // literal, length (2 << 2) | 0 = 8 → length = 2+1 = 3
        'a',
        'b',
        'c',
        0x0A, // copy-2: (2 << 2) | 0x02 → length = 2+1 = 3
        0x03, 0x00, // offset = 3 (LE)
    };
    const result = try decompressSnappy(&compressed, std.testing.allocator);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("abcabc", result);
}

test "LZ4 frame: empty block decompresses to empty" {
    // Minimal LZ4 frame: magic + FLG + BD + HC + end mark(4 zeros)
    const frame = [_]u8{
        0x04, 0x22, 0x4D, 0x18, // magic
        0x60, // FLG: version=01, no content size, no dict
        0x40, // BD: block max = 64KB
        0x82, // HC (checksum — simplified, may not match real)
        0x00, 0x00, 0x00, 0x00, // end mark
    };
    const result = try decompressLz4(&frame, std.testing.allocator);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "LZ4 frame: uncompressed block" {
    // LZ4 frame with one uncompressed block containing "hello"
    const frame = [_]u8{
        0x04, 0x22, 0x4D, 0x18, // magic
        0x60, // FLG
        0x40, // BD
        0x82, // HC
        0x05, 0x00, 0x00, 0x80, // block header: size=5 | 0x80000000 (uncompressed)
        'h',  'e',  'l',  'l',
        'o',
        0x00, 0x00, 0x00, 0x00, // end mark
    };
    const result = try decompressLz4(&frame, std.testing.allocator);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("hello", result);
}

test "LZ4 frame: bad magic fails" {
    const frame = [_]u8{ 0x00, 0x00, 0x00, 0x00, 0x60, 0x40, 0x82 };
    try std.testing.expectError(error.DecompressionFailed, decompressLz4(&frame, std.testing.allocator));
}

test "ZSTD: too small input fails" {
    try std.testing.expectError(error.DecompressionFailed, decompressZstd(&[_]u8{ 0x00, 0x01 }, std.testing.allocator));
}
