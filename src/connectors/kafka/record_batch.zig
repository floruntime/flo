//! RecordBatch v2 Decoder
//!
//! Decodes Kafka RecordBatch format (MessageSet v2, Kafka 0.11+).
//! Each batch contains a header, compression info, and N records encoded
//! with varint-length-prefixed fields.
//!
//! Phase 1 supports: no compression (0) and GZIP (1).

const std = @import("std");
const Allocator = std.mem.Allocator;
const codec = @import("codec.zig");
const KafkaReader = codec.KafkaReader;
const compress = @import("compress.zig");

const log = @import("stdx").log;

// =============================================================================
// RecordBatch Header (~61 bytes fixed overhead)
// =============================================================================

pub const CompressionType = enum(u3) {
    none = 0,
    gzip = 1,
    snappy = 2,
    lz4 = 3,
    zstd = 4,
};

pub const RecordBatchHeader = struct {
    base_offset: i64,
    batch_length: i32,
    partition_leader_epoch: i32,
    magic: i8,
    crc: u32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    record_count: i32,

    pub fn compression(self: RecordBatchHeader) CompressionType {
        return @enumFromInt(@as(u3, @truncate(@as(u16, @bitCast(self.attributes)))));
    }

    pub fn isTransactional(self: RecordBatchHeader) bool {
        return (self.attributes & 0x10) != 0;
    }

    pub fn isControlBatch(self: RecordBatchHeader) bool {
        return (self.attributes & 0x20) != 0;
    }
};

/// A single decoded record within a batch.
pub const KafkaRecord = struct {
    offset_delta: i32,
    timestamp_delta: i64,
    key: ?[]const u8,
    value: ?[]const u8,
    headers: []RecordHeader,
};

pub const RecordHeader = struct {
    key: []const u8,
    value: ?[]const u8,
};

/// Iterator over records in raw batch data (possibly multiple batches).
pub const RecordBatchIterator = struct {
    data: []const u8,
    pos: usize,
    /// Decompressed data backing (if needed)
    decompressed: ?[]u8,
    allocator: Allocator,

    // Current batch state
    batch_header: ?RecordBatchHeader,
    records_data: ?[]const u8, // decompressed records within current batch
    records_reader: ?KafkaReader,
    records_remaining: i32,

    pub fn init(data: []const u8, allocator: Allocator) RecordBatchIterator {
        return .{
            .data = data,
            .pos = 0,
            .decompressed = null,
            .allocator = allocator,
            .batch_header = null,
            .records_data = null,
            .records_reader = null,
            .records_remaining = 0,
        };
    }

    pub fn deinit(self: *RecordBatchIterator) void {
        if (self.decompressed) |d| self.allocator.free(d);
    }

    /// Returns the next record, or null when all batches/records are exhausted.
    pub fn next(self: *RecordBatchIterator) !?DecodedRecord {
        while (true) {
            // Try to read from current batch
            if (self.records_remaining > 0) {
                if (self.records_reader) |*reader| {
                    const record = try decodeRecord(reader);
                    self.records_remaining -= 1;
                    return .{
                        .header = self.batch_header.?,
                        .record = record,
                    };
                }
            }

            // Need to advance to next batch
            if (!try self.advanceBatch()) return null;
        }
    }

    fn advanceBatch(self: *RecordBatchIterator) !bool {
        // Free previous decompressed buffer
        if (self.decompressed) |d| {
            self.allocator.free(d);
            self.decompressed = null;
        }

        // Check if we have enough data for a batch header
        if (self.pos + BATCH_HEADER_SIZE > self.data.len) return false;

        var header_reader = KafkaReader.init(self.data[self.pos..]);
        const header = try decodeBatchHeader(&header_reader);

        if (header.magic != 2) {
            log.err("Unsupported RecordBatch magic byte: {d}, expected 2", .{header.magic});
            return error.UnsupportedBatchVersion;
        }

        // The total batch size = 12 (baseOffset + batchLength) + batchLength
        const total_batch_size: usize = 12 + @as(usize, @intCast(header.batch_length));
        if (self.pos + total_batch_size > self.data.len) return false;

        // Records start after the fixed header (61 bytes from start of batch)
        const records_start = self.pos + BATCH_HEADER_SIZE;
        const records_end = self.pos + total_batch_size;

        if (records_start > records_end) return false;

        const raw_records = self.data[records_start..records_end];

        // Handle compression
        const comp = header.compression();
        const records_data = switch (comp) {
            .none => raw_records,
            .gzip => try decompressGzip(raw_records, self.allocator, &self.decompressed),
            .snappy => blk: {
                const decompressed = compress.decompressSnappy(raw_records, self.allocator) catch |err| {
                    log.err("Snappy decompression failed: {}", .{err});
                    return error.DecompressionFailed;
                };
                self.decompressed = decompressed;
                break :blk decompressed;
            },
            .lz4 => blk: {
                const decompressed = compress.decompressLz4(raw_records, self.allocator) catch |err| {
                    log.err("LZ4 decompression failed: {}", .{err});
                    return error.DecompressionFailed;
                };
                self.decompressed = decompressed;
                break :blk decompressed;
            },
            .zstd => blk: {
                const decompressed = compress.decompressZstd(raw_records, self.allocator) catch |err| {
                    log.err("ZSTD decompression failed: {}", .{err});
                    return error.DecompressionFailed;
                };
                self.decompressed = decompressed;
                break :blk decompressed;
            },
        };

        self.batch_header = header;
        self.records_data = records_data;
        self.records_reader = KafkaReader.init(records_data);
        self.records_remaining = header.record_count;
        self.pos = self.pos + total_batch_size;

        // Skip control batches (transaction markers)
        if (header.isControlBatch()) {
            self.records_remaining = 0;
            return self.advanceBatch();
        }

        return true;
    }
};

/// A decoded record with its batch context.
pub const DecodedRecord = struct {
    header: RecordBatchHeader,
    record: KafkaRecord,
};

// =============================================================================
// Batch Header Decoding
// =============================================================================

/// Fixed size of batch header: 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 = 61 bytes
/// But it's offset from the start: baseOffset(8) + batchLength(4) + rest(49) = 61
const BATCH_HEADER_SIZE: usize = 61;

fn decodeBatchHeader(reader: *KafkaReader) !RecordBatchHeader {
    const base_offset = try reader.readInt64();
    const batch_length = try reader.readInt32();
    const partition_leader_epoch = try reader.readInt32();
    const magic = try reader.readInt8();
    const crc = @as(u32, @bitCast(try reader.readInt32()));
    const attributes = try reader.readInt16();
    const last_offset_delta = try reader.readInt32();
    const base_timestamp = try reader.readInt64();
    const max_timestamp = try reader.readInt64();
    const producer_id = try reader.readInt64();
    const producer_epoch = try reader.readInt16();
    const base_sequence = try reader.readInt32();
    const record_count = try reader.readInt32();

    return .{
        .base_offset = base_offset,
        .batch_length = batch_length,
        .partition_leader_epoch = partition_leader_epoch,
        .magic = magic,
        .crc = crc,
        .attributes = attributes,
        .last_offset_delta = last_offset_delta,
        .base_timestamp = base_timestamp,
        .max_timestamp = max_timestamp,
        .producer_id = producer_id,
        .producer_epoch = producer_epoch,
        .base_sequence = base_sequence,
        .record_count = record_count,
    };
}

// =============================================================================
// Record Decoding (varint-encoded)
// =============================================================================

fn decodeRecord(reader: *KafkaReader) !KafkaRecord {
    // Record length (varint, signed zigzag)
    _ = try reader.readVarInt(); // record length — we rely on field-by-field parsing

    // Attributes (int8, currently unused)
    _ = try reader.readInt8();

    // Timestamp delta (varint, signed zigzag)
    const timestamp_delta = try reader.readVarInt();

    // Offset delta (varint, signed zigzag)
    const offset_delta_raw = try reader.readVarInt();

    // Key (varint length + bytes)
    const key = try readVarBytes(reader);

    // Value (varint length + bytes)
    const value = try readVarBytes(reader);

    // Headers
    const header_count_raw = try reader.readUnsignedVarInt();
    const header_count: usize = @intCast(header_count_raw);

    // We return a zero-length slice for headers to avoid allocation
    // Callers who need headers must re-decode or we'd need an allocator here
    var headers_buf: [0]RecordHeader = .{};

    // Skip header bytes
    for (0..header_count) |_| {
        // header key (varString)
        const key_len_raw = try reader.readUnsignedVarInt();
        const key_len: usize = @intCast(key_len_raw);
        try reader.skip(key_len);
        // header value (varBytes)
        const val_len_raw = try reader.readVarInt();
        if (val_len_raw >= 0) {
            try reader.skip(@intCast(val_len_raw));
        }
    }

    return .{
        .offset_delta = @intCast(offset_delta_raw),
        .timestamp_delta = timestamp_delta,
        .key = key,
        .value = value,
        .headers = &headers_buf,
    };
}

/// Read varint-length-prefixed bytes. Returns null if length is -1.
fn readVarBytes(reader: *KafkaReader) !?[]const u8 {
    const len = try reader.readVarInt();
    if (len < 0) return null;
    if (len == 0) return &[_]u8{};
    const ulen: usize = @intCast(len);
    if (reader.remaining() < ulen) return error.EndOfBuffer;
    const offset = reader.offset();
    try reader.skip(ulen);
    return reader.slice()[offset..][0..ulen];
}

// =============================================================================
// GZIP Decompression
// =============================================================================

fn decompressGzip(data: []const u8, allocator: Allocator, out_buf: *?[]u8) ![]const u8 {
    // Phase 1: Use std.compress.flate.Decompress with .gzip container
    // The gzip format wraps raw deflate with a 10-byte header and 8-byte trailer.
    // We need at least a minimal gzip stream.
    if (data.len < 10) return error.DecompressionFailed;

    // Validate gzip magic bytes
    if (data[0] != 0x1f or data[1] != 0x8b) return error.DecompressionFailed;

    // Skip the gzip header (10 bytes minimum) and trailer (8 bytes) to get raw deflate
    // Method must be 8 (deflate)
    if (data[2] != 0x08) return error.DecompressionFailed;

    // For simplicity, skip the header and decompress the raw deflate stream.
    // A full gzip header is 10 bytes when FLG == 0.
    const flags = data[3];
    var header_end: usize = 10;

    // FEXTRA
    if (flags & 0x04 != 0) {
        if (header_end + 2 > data.len) return error.DecompressionFailed;
        const xlen = @as(u16, data[header_end]) | (@as(u16, data[header_end + 1]) << 8);
        header_end += 2 + xlen;
    }
    // FNAME
    if (flags & 0x08 != 0) {
        while (header_end < data.len and data[header_end] != 0) header_end += 1;
        header_end += 1; // skip null terminator
    }
    // FCOMMENT
    if (flags & 0x10 != 0) {
        while (header_end < data.len and data[header_end] != 0) header_end += 1;
        header_end += 1;
    }
    // FHCRC
    if (flags & 0x02 != 0) header_end += 2;

    if (header_end >= data.len) return error.DecompressionFailed;

    // Strip the 8-byte gzip trailer (CRC32 + ISIZE)
    const trailer_size: usize = 8;
    if (data.len < header_end + trailer_size) return error.DecompressionFailed;
    const deflate_data = data[header_end .. data.len - trailer_size];

    // Use std.compress.flate.Decompress with .raw container on the deflate payload
    var input_reader: std.Io.Reader = .fixed(deflate_data);
    var decompress_buf: [std.compress.flate.max_window_len]u8 = undefined;
    var decomp: std.compress.flate.Decompress = .init(&input_reader, .raw, &decompress_buf);

    // Read all decompressed data
    var result: std.ArrayList(u8) = .{};
    defer result.deinit(allocator);

    var buf: [4096]u8 = undefined;
    while (true) {
        const n = decomp.reader.readSliceShort(&buf) catch |err| {
            log.err("GZIP decompression failed: {}", .{err});
            return error.DecompressionFailed;
        };
        if (n == 0) break;
        try result.appendSlice(allocator, buf[0..n]);
    }

    const owned = try result.toOwnedSlice(allocator);
    out_buf.* = owned;
    return owned;
}

// =============================================================================
// Tests
// =============================================================================

test "CompressionType from attributes" {
    const h = RecordBatchHeader{
        .base_offset = 0,
        .batch_length = 0,
        .partition_leader_epoch = 0,
        .magic = 2,
        .crc = 0,
        .attributes = 0, // no compression
        .last_offset_delta = 0,
        .base_timestamp = 0,
        .max_timestamp = 0,
        .producer_id = 0,
        .producer_epoch = 0,
        .base_sequence = 0,
        .record_count = 0,
    };
    try std.testing.expectEqual(CompressionType.none, h.compression());

    var h2 = h;
    h2.attributes = 1; // gzip
    try std.testing.expectEqual(CompressionType.gzip, h2.compression());

    var h3 = h;
    h3.attributes = 2; // snappy
    try std.testing.expectEqual(CompressionType.snappy, h3.compression());
}

test "RecordBatchHeader flags" {
    var h = RecordBatchHeader{
        .base_offset = 0,
        .batch_length = 0,
        .partition_leader_epoch = 0,
        .magic = 2,
        .crc = 0,
        .attributes = 0x10, // transactional
        .last_offset_delta = 0,
        .base_timestamp = 0,
        .max_timestamp = 0,
        .producer_id = 0,
        .producer_epoch = 0,
        .base_sequence = 0,
        .record_count = 0,
    };
    try std.testing.expect(h.isTransactional());
    try std.testing.expect(!h.isControlBatch());

    h.attributes = 0x20; // control
    try std.testing.expect(!h.isTransactional());
    try std.testing.expect(h.isControlBatch());

    h.attributes = 0x30; // both
    try std.testing.expect(h.isTransactional());
    try std.testing.expect(h.isControlBatch());
}

test "decodeBatchHeader from raw bytes" {
    // Construct a minimal valid batch header (61 bytes)
    var buf: [61]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const writer = fbs.writer();
    writer.writeInt(i64, 100, .big) catch unreachable; // base_offset
    writer.writeInt(i32, 49, .big) catch unreachable; // batch_length (61-12=49 bytes after this)
    writer.writeInt(i32, 0, .big) catch unreachable; // partition_leader_epoch
    writer.writeInt(i8, 2, .big) catch unreachable; // magic
    writer.writeInt(u32, 0xDEADBEEF, .big) catch unreachable; // crc
    writer.writeInt(i16, 0, .big) catch unreachable; // attributes (no compression)
    writer.writeInt(i32, 2, .big) catch unreachable; // last_offset_delta
    writer.writeInt(i64, 1000000, .big) catch unreachable; // base_timestamp
    writer.writeInt(i64, 1000100, .big) catch unreachable; // max_timestamp
    writer.writeInt(i64, -1, .big) catch unreachable; // producer_id
    writer.writeInt(i16, -1, .big) catch unreachable; // producer_epoch
    writer.writeInt(i32, -1, .big) catch unreachable; // base_sequence
    writer.writeInt(i32, 3, .big) catch unreachable; // record_count

    var reader = KafkaReader.init(&buf);
    const header = try decodeBatchHeader(&reader);

    try std.testing.expectEqual(@as(i64, 100), header.base_offset);
    try std.testing.expectEqual(@as(i32, 49), header.batch_length);
    try std.testing.expectEqual(@as(i8, 2), header.magic);
    try std.testing.expectEqual(@as(u32, 0xDEADBEEF), header.crc);
    try std.testing.expectEqual(@as(i32, 2), header.last_offset_delta);
    try std.testing.expectEqual(@as(i64, 1000000), header.base_timestamp);
    try std.testing.expectEqual(@as(i64, 1000100), header.max_timestamp);
    try std.testing.expectEqual(@as(i32, 3), header.record_count);
    try std.testing.expectEqual(CompressionType.none, header.compression());
}

test "decodeRecord from varint-encoded bytes" {
    // Build a minimal record:
    // length(varint), attributes(i8), timestampDelta(varint), offsetDelta(varint),
    // key(varBytes), value(varBytes), headerCount(uvarint)
    var buf: [64]u8 = undefined;
    var pos: usize = 0;

    // We'll build the record content first, then prepend the length
    var content: [60]u8 = undefined;
    var cpos: usize = 0;

    // attributes
    content[cpos] = 0;
    cpos += 1;

    // timestampDelta = 50 → zigzag = 100
    cpos += writeZigzag(content[cpos..], 50);

    // offsetDelta = 1 → zigzag = 2
    cpos += writeZigzag(content[cpos..], 1);

    // key = "k1" → varint(2) + "k1"
    cpos += writeZigzag(content[cpos..], 2);
    @memcpy(content[cpos..][0..2], "k1");
    cpos += 2;

    // value = "hello" → varint(5) + "hello"
    cpos += writeZigzag(content[cpos..], 5);
    @memcpy(content[cpos..][0..5], "hello");
    cpos += 5;

    // headers count = 0 (uvarint)
    content[cpos] = 0;
    cpos += 1;

    // Now write length as zigzag varint
    pos += writeZigzag(buf[pos..], @intCast(cpos));
    @memcpy(buf[pos..][0..cpos], content[0..cpos]);
    pos += cpos;

    var reader = KafkaReader.init(buf[0..pos]);
    const record = try decodeRecord(&reader);

    try std.testing.expectEqual(@as(i32, 1), record.offset_delta);
    try std.testing.expectEqual(@as(i64, 50), record.timestamp_delta);
    try std.testing.expectEqualStrings("k1", record.key.?);
    try std.testing.expectEqualStrings("hello", record.value.?);
}

test "decodeRecord with null key" {
    var buf: [64]u8 = undefined;
    var pos: usize = 0;

    var content: [60]u8 = undefined;
    var cpos: usize = 0;

    content[cpos] = 0; // attributes
    cpos += 1;
    cpos += writeZigzag(content[cpos..], 0); // timestampDelta
    cpos += writeZigzag(content[cpos..], 0); // offsetDelta
    cpos += writeZigzag(content[cpos..], -1); // null key
    cpos += writeZigzag(content[cpos..], 3); // value length
    @memcpy(content[cpos..][0..3], "abc");
    cpos += 3;
    content[cpos] = 0; // 0 headers
    cpos += 1;

    pos += writeZigzag(buf[pos..], @intCast(cpos));
    @memcpy(buf[pos..][0..cpos], content[0..cpos]);
    pos += cpos;

    var reader = KafkaReader.init(buf[0..pos]);
    const record = try decodeRecord(&reader);

    try std.testing.expect(record.key == null);
    try std.testing.expectEqualStrings("abc", record.value.?);
}

test "RecordBatchIterator empty data" {
    var iter = RecordBatchIterator.init(&[_]u8{}, std.testing.allocator);
    defer iter.deinit();
    const result = try iter.next();
    try std.testing.expect(result == null);
}

test "RecordBatchIterator single batch with records" {
    // Construct a complete RecordBatch with 2 records, no compression
    var batch_buf: [256]u8 = undefined;
    var batch_pos: usize = 0;

    // We'll build the records first to know their size
    var records_buf: [128]u8 = undefined;
    var rpos: usize = 0;

    // Record 0: key="k0", value="v0"
    rpos += buildTestRecord(&records_buf, rpos, 0, 0, "k0", "v0");
    // Record 1: key="k1", value="v1"
    rpos += buildTestRecord(&records_buf, rpos, 10, 1, "k1", "v1");

    const records_size = rpos;
    // batch_length = header_after_batchLength(49) + records_size
    const batch_length: i32 = @intCast(49 + records_size);

    // Write batch header
    var hdr_writer = std.io.fixedBufferStream(batch_buf[0..]);
    const hw = hdr_writer.writer();
    hw.writeInt(i64, 42, .big) catch unreachable; // base_offset
    hw.writeInt(i32, batch_length, .big) catch unreachable;
    hw.writeInt(i32, 0, .big) catch unreachable; // partition_leader_epoch
    hw.writeInt(i8, 2, .big) catch unreachable; // magic
    hw.writeInt(u32, 0, .big) catch unreachable; // crc
    hw.writeInt(i16, 0, .big) catch unreachable; // attributes (no compression)
    hw.writeInt(i32, 1, .big) catch unreachable; // last_offset_delta
    hw.writeInt(i64, 1000, .big) catch unreachable; // base_timestamp
    hw.writeInt(i64, 1010, .big) catch unreachable; // max_timestamp
    hw.writeInt(i64, -1, .big) catch unreachable; // producer_id
    hw.writeInt(i16, -1, .big) catch unreachable; // producer_epoch
    hw.writeInt(i32, -1, .big) catch unreachable; // base_sequence
    hw.writeInt(i32, 2, .big) catch unreachable; // record_count
    batch_pos = hdr_writer.pos;

    // Append records
    @memcpy(batch_buf[batch_pos..][0..records_size], records_buf[0..records_size]);
    batch_pos += records_size;

    // Iterate
    var iter = RecordBatchIterator.init(batch_buf[0..batch_pos], std.testing.allocator);
    defer iter.deinit();

    // Record 0
    const r0_opt = try iter.next();
    try std.testing.expect(r0_opt != null);
    const r0 = r0_opt.?;
    try std.testing.expectEqual(@as(i64, 42), r0.header.base_offset);
    try std.testing.expectEqual(@as(i32, 0), r0.record.offset_delta);
    try std.testing.expectEqualStrings("k0", r0.record.key.?);
    try std.testing.expectEqualStrings("v0", r0.record.value.?);

    // Record 1
    const r1_opt = try iter.next();
    try std.testing.expect(r1_opt != null);
    const r1 = r1_opt.?;
    try std.testing.expectEqual(@as(i32, 1), r1.record.offset_delta);
    try std.testing.expectEqualStrings("k1", r1.record.key.?);
    try std.testing.expectEqualStrings("v1", r1.record.value.?);

    // No more
    try std.testing.expect(try iter.next() == null);
}

// =============================================================================
// Test Helpers
// =============================================================================

fn writeZigzag(buf: []u8, value: i64) usize {
    const encoded: u64 = @bitCast((value << 1) ^ (value >> 63));
    return writeUnsignedVarInt(buf, encoded);
}

fn writeUnsignedVarInt(buf: []u8, value: u64) usize {
    var v = value;
    var i: usize = 0;
    while (v > 0x7F) : (i += 1) {
        buf[i] = @truncate((v & 0x7F) | 0x80);
        v >>= 7;
    }
    buf[i] = @truncate(v & 0x7F);
    return i + 1;
}

fn buildTestRecord(buf: []u8, start: usize, ts_delta: i64, offset_delta: i32, key: []const u8, value: []const u8) usize {
    var content: [128]u8 = undefined;
    var cpos: usize = 0;

    content[cpos] = 0; // attributes
    cpos += 1;
    cpos += writeZigzag(content[cpos..], ts_delta);
    cpos += writeZigzag(content[cpos..], offset_delta);
    cpos += writeZigzag(content[cpos..], @intCast(key.len));
    @memcpy(content[cpos..][0..key.len], key);
    cpos += key.len;
    cpos += writeZigzag(content[cpos..], @intCast(value.len));
    @memcpy(content[cpos..][0..value.len], value);
    cpos += value.len;
    content[cpos] = 0; // 0 headers
    cpos += 1;

    // Write length varint + content
    var pos: usize = start;
    pos += writeZigzag(buf[pos..], @intCast(cpos));
    @memcpy(buf[pos..][0..cpos], content[0..cpos]);
    pos += cpos;
    return pos - start;
}
