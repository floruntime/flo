//! UAL Entry — Unified Append Log entry format
//!
//! Every mutation in Flo (KV put, stream append, queue enqueue, etc.)
//! is serialized as a UAL entry. The format has a fixed 40-byte header
//! followed by a variable-length payload.
//!
//! ## Header Layout (40 bytes)
//!
//! ```
//! magic:        u32  (0x0A10_E001)
//! version:      u8   (1)
//! entry_type:   u8   (EntryType enum)
//! flags:        u16  (compression, etc.)
//! term:         u64  (Raft term)
//! index:        u64  (Raft/UAL monotonic index, starts at 1)
//! timestamp_ns: u64  (wall clock nanoseconds)
//! payload_len:  u32  (bytes of payload following header)
//! crc32c:       u32  (CRC32C of header[0..36] + payload)
//! ```
//!
//! ## CRC Computation
//!
//! CRC32C (Castagnoli) covers the first 36 bytes of the header (everything
//! except the crc32c field itself) plus the entire payload. This allows
//! a single-pass streaming checksum.
//!
//! ## Payload
//!
//! For key-value commands (kv_put, kv_delete, stream_append, queue_enqueue,
//! etc.), the payload uses a common "command payload" layout:
//!
//! ```
//! namespace_hash: u32
//! key_length:     u16
//! value_length:   u32
//! key:            [key_length]u8
//! value:          [value_length]u8
//! ```
//!
//! Raft control entries (noop, config) may have different payload formats.

const std = @import("std");
const checksum_mod = @import("../../util/checksum.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

pub const ENTRY_MAGIC: u32 = 0x0A10_E001;
pub const ENTRY_VERSION: u8 = 1;
pub const HEADER_SIZE: usize = 40;
/// Bytes of header covered by CRC (everything except the crc32c field).
pub const CRC_HEADER_SIZE: usize = 36;

// ═══════════════════════════════════════════════════════════════════════════════
// EntryType
// ═══════════════════════════════════════════════════════════════════════════════

pub const EntryType = enum(u8) {
    // ── KV ──
    kv_put = 0x01,
    kv_delete = 0x02,
    kv_batch = 0x03,

    // ── Stream ──
    stream_append = 0x10,
    stream_trim = 0x11,

    // ── Queue ──
    queue_enqueue = 0x20,
    queue_ack = 0x21,
    queue_nack = 0x22,
    queue_lease = 0x23,

    // ── TimeSeries ──
    ts_write = 0x30,
    ts_write_batch = 0x31,

    // ── Consumer Groups ──
    cg_commit = 0x40,
    cg_create = 0x41,
    cg_delete = 0x42,

    // ── Raft Control ──
    raft_config = 0xF0,
    raft_noop = 0xF1,
    raft_snapshot = 0xF2,

    // ── Workflow ──
    workflow_create = 0x50,
    workflow_start = 0x51,

    // ── Namespace ──
    namespace_create = 0x60,
    namespace_delete = 0x61,
    namespace_config = 0x62,

    // ── Actions ──
    action_register = 0x70,
    action_delete = 0x71,
    action_invoke = 0x72,
    action_update_run = 0x73,

    // ── Processing ──
    processing_submit = 0x80,
    processing_stop = 0x81,
    processing_cancel = 0x82,
    processing_savepoint = 0x83,
    processing_rescale = 0x84,

    // ── Checkpoint ──
    checkpoint = 0xE0,

    /// Returns true if this type carries a key-value command payload.
    pub fn hasKeyValue(self: EntryType) bool {
        return switch (self) {
            .kv_put, .kv_delete, .kv_batch => true,
            .stream_append, .stream_trim => true,
            .queue_enqueue, .queue_ack, .queue_nack, .queue_lease => true,
            .ts_write, .ts_write_batch => true,
            .cg_commit, .cg_create, .cg_delete => true,
            .workflow_create, .workflow_start => true,
            .namespace_create, .namespace_delete, .namespace_config => true,
            .action_register, .action_delete, .action_invoke, .action_update_run => true,
            .processing_submit, .processing_stop, .processing_cancel, .processing_savepoint, .processing_rescale => true,
            .raft_config, .raft_noop, .raft_snapshot, .checkpoint => false,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Flags
// ═══════════════════════════════════════════════════════════════════════════════

pub const Flags = struct {
    pub const NONE: u16 = 0;
    pub const COMPRESSED_LZ4: u16 = 1 << 0;
    pub const COMPRESSED_ZSTD: u16 = 1 << 1;
    pub const HAS_TTL: u16 = 1 << 2;
    pub const TOMBSTONE: u16 = 1 << 3;
};

// ═══════════════════════════════════════════════════════════════════════════════
// Entry Header (extern for exact memory layout)
// ═══════════════════════════════════════════════════════════════════════════════

pub const Header = extern struct {
    magic: u32 align(1),
    version: u8 align(1),
    entry_type: u8 align(1),
    flags: u16 align(1),
    term: u64 align(1),
    index: u64 align(1),
    timestamp_ns: u64 align(1),
    payload_len: u32 align(1),
    crc32c: u32 align(1),

    comptime {
        if (@sizeOf(Header) != HEADER_SIZE) {
            @compileError("Header must be exactly 40 bytes");
        }
    }

    /// Get a byte view of the header.
    pub fn asBytes(self: *const Header) *const [HEADER_SIZE]u8 {
        return @ptrCast(self);
    }

    /// Get a mutable byte view of the header.
    pub fn asBytesMut(self: *Header) *[HEADER_SIZE]u8 {
        return @ptrCast(self);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Command Payload Layout
// ═══════════════════════════════════════════════════════════════════════════════

/// Common payload prefix for key-value command entries.
/// Fixed 10-byte prefix: namespace_hash(4) + key_length(2) + value_length(4)
pub const COMMAND_PREFIX_SIZE: usize = 10;

pub const CommandPayload = struct {
    namespace_hash: u32,
    key_length: u16,
    value_length: u32,
    key: []const u8,
    value: []const u8,

    /// Total serialized size of this payload.
    pub fn serializedSize(self: *const CommandPayload) u32 {
        return @intCast(COMMAND_PREFIX_SIZE + self.key.len + self.value.len);
    }

    /// Serialize into a buffer. Returns the number of bytes written.
    pub fn serialize(self: *const CommandPayload, buf: []u8) ?usize {
        const total = self.serializedSize();
        if (buf.len < total) return null;

        std.mem.writeInt(u32, buf[0..4], self.namespace_hash, .little);
        std.mem.writeInt(u16, buf[4..6], self.key_length, .little);
        std.mem.writeInt(u32, buf[6..10], self.value_length, .little);

        if (self.key.len > 0) {
            @memcpy(buf[10 .. 10 + self.key.len], self.key);
        }
        const value_start = 10 + self.key.len;
        if (self.value.len > 0) {
            @memcpy(buf[value_start .. value_start + self.value.len], self.value);
        }

        return @intCast(total);
    }

    /// Deserialize from a byte slice.
    pub fn deserialize(data: []const u8) ?CommandPayload {
        if (data.len < COMMAND_PREFIX_SIZE) return null;

        const ns_hash = std.mem.readInt(u32, data[0..4], .little);
        const key_len = std.mem.readInt(u16, data[4..6], .little);
        const val_len = std.mem.readInt(u32, data[6..10], .little);

        const total: usize = COMMAND_PREFIX_SIZE + key_len + val_len;
        if (data.len < total) return null;

        return .{
            .namespace_hash = ns_hash,
            .key_length = key_len,
            .value_length = val_len,
            .key = data[10 .. 10 + key_len],
            .value = data[10 + key_len .. 10 + key_len + val_len],
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Entry (deserialized view)
// ═══════════════════════════════════════════════════════════════════════════════

pub const Entry = struct {
    header: Header,
    payload: []const u8,

    /// Total serialized size (header + payload).
    pub fn totalSize(self: *const Entry) usize {
        return HEADER_SIZE + self.payload.len;
    }

    /// Compute the CRC32C over header[0..36] + payload.
    pub fn computeCrc(self: *const Entry) u32 {
        var stream = checksum_mod.ChecksumStream.init();
        // Feed first 36 bytes of header (excluding the crc32c field)
        const hdr_bytes = self.header.asBytes();
        stream.add(hdr_bytes[0..CRC_HEADER_SIZE]);
        // Feed payload
        if (self.payload.len > 0) {
            stream.add(self.payload);
        }
        return stream.checksum();
    }

    /// Validate the entry: check magic, version, CRC, lengths.
    pub fn validate(self: *const Entry) error{ InvalidMagic, InvalidVersion, InvalidCrc, InvalidLength }!void {
        if (self.header.magic != ENTRY_MAGIC) return error.InvalidMagic;
        if (self.header.version != ENTRY_VERSION) return error.InvalidVersion;
        if (self.header.payload_len != @as(u32, @intCast(self.payload.len))) return error.InvalidLength;

        const expected_crc = self.computeCrc();
        if (self.header.crc32c != expected_crc) return error.InvalidCrc;
    }

    /// Parse the payload as a command (key-value) payload.
    pub fn commandPayload(self: *const Entry) ?CommandPayload {
        const et: EntryType = @enumFromInt(self.header.entry_type);
        if (!et.hasKeyValue()) return null;
        return CommandPayload.deserialize(self.payload);
    }

    /// Serialize the complete entry (header + payload) into a buffer.
    /// Returns the number of bytes written, or null if buffer too small.
    pub fn serialize(self: *const Entry, buf: []u8) ?usize {
        const total = self.totalSize();
        if (buf.len < total) return null;

        const hdr_bytes = self.header.asBytes();
        @memcpy(buf[0..HEADER_SIZE], hdr_bytes);
        if (self.payload.len > 0) {
            @memcpy(buf[HEADER_SIZE .. HEADER_SIZE + self.payload.len], self.payload);
        }
        return total;
    }

    /// Deserialize an entry from a byte buffer. The returned entry borrows
    /// from the buffer (zero-copy for payload).
    pub fn deserialize(data: []const u8) ?Entry {
        if (data.len < HEADER_SIZE) return null;

        const hdr: *const Header = @ptrCast(@alignCast(data[0..HEADER_SIZE]));
        const payload_len: usize = hdr.payload_len;

        if (data.len < HEADER_SIZE + payload_len) return null;

        return .{
            .header = hdr.*,
            .payload = data[HEADER_SIZE .. HEADER_SIZE + payload_len],
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Builder — convenient entry construction
// ═══════════════════════════════════════════════════════════════════════════════

/// Build a UAL entry with correct magic, version, payload length, and CRC.
pub fn buildEntry(
    entry_type: EntryType,
    flags: u16,
    term: u64,
    index: u64,
    timestamp_ns: u64,
    payload: []const u8,
) Entry {
    var entry = Entry{
        .header = .{
            .magic = ENTRY_MAGIC,
            .version = ENTRY_VERSION,
            .entry_type = @intFromEnum(entry_type),
            .flags = flags,
            .term = term,
            .index = index,
            .timestamp_ns = timestamp_ns,
            .payload_len = @intCast(payload.len),
            .crc32c = 0, // will be computed below
        },
        .payload = payload,
    };
    entry.header.crc32c = entry.computeCrc();
    return entry;
}

/// Build a key-value command entry. The caller must provide a scratch buffer
/// for the serialized payload (CommandPayload prefix + key + value).
pub fn buildCommandEntry(
    entry_type: EntryType,
    flags: u16,
    term: u64,
    index: u64,
    timestamp_ns: u64,
    namespace_hash: u32,
    key: []const u8,
    value: []const u8,
    payload_buf: []u8,
) ?Entry {
    const cmd = CommandPayload{
        .namespace_hash = namespace_hash,
        .key_length = @intCast(key.len),
        .value_length = @intCast(value.len),
        .key = key,
        .value = value,
    };
    const written = cmd.serialize(payload_buf) orelse return null;
    return buildEntry(entry_type, flags, term, index, timestamp_ns, payload_buf[0..written]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "entry: header size is 40 bytes" {
    try std.testing.expectEqual(@as(usize, 40), @sizeOf(Header));
}

test "entry: buildEntry sets magic, version, CRC" {
    const payload = "hello, world";
    const entry = buildEntry(.kv_put, Flags.NONE, 1, 42, 1234567890, payload);

    try std.testing.expectEqual(ENTRY_MAGIC, entry.header.magic);
    try std.testing.expectEqual(ENTRY_VERSION, entry.header.version);
    try std.testing.expectEqual(@as(u8, 0x01), entry.header.entry_type);
    try std.testing.expectEqual(@as(u64, 1), entry.header.term);
    try std.testing.expectEqual(@as(u64, 42), entry.header.index);
    try std.testing.expectEqual(@as(u32, 12), entry.header.payload_len);
    try std.testing.expect(entry.header.crc32c != 0);
}

test "entry: validate accepts correct entry" {
    const payload = "test payload";
    const entry = buildEntry(.kv_put, Flags.NONE, 1, 1, 999, payload);
    try entry.validate();
}

test "entry: validate rejects bad magic" {
    const payload = "test";
    var entry = buildEntry(.kv_put, Flags.NONE, 1, 1, 0, payload);
    entry.header.magic = 0xDEADDEAD;
    try std.testing.expectError(error.InvalidMagic, entry.validate());
}

test "entry: validate rejects bad CRC" {
    const payload = "test";
    var entry = buildEntry(.kv_put, Flags.NONE, 1, 1, 0, payload);
    entry.header.crc32c ^= 0xFF; // corrupt CRC
    try std.testing.expectError(error.InvalidCrc, entry.validate());
}

test "entry: validate rejects bad version" {
    const payload = "test";
    var entry = buildEntry(.kv_put, Flags.NONE, 1, 1, 0, payload);
    entry.header.version = 99;
    // CRC will also be wrong, but version is checked first
    try std.testing.expectError(error.InvalidVersion, entry.validate());
}

test "entry: serialize and deserialize roundtrip" {
    const payload = "roundtrip data";
    const original = buildEntry(.stream_append, Flags.HAS_TTL, 3, 100, 5555, payload);

    var buf: [256]u8 = undefined;
    const written = original.serialize(&buf);
    try std.testing.expect(written != null);
    try std.testing.expectEqual(@as(usize, HEADER_SIZE + 14), written.?);

    const recovered = Entry.deserialize(buf[0..written.?]);
    try std.testing.expect(recovered != null);

    const e = recovered.?;
    try std.testing.expectEqual(ENTRY_MAGIC, e.header.magic);
    try std.testing.expectEqual(@as(u8, 0x10), e.header.entry_type); // stream_append
    try std.testing.expectEqual(@as(u64, 3), e.header.term);
    try std.testing.expectEqual(@as(u64, 100), e.header.index);
    try std.testing.expectEqualStrings("roundtrip data", e.payload);
    try e.validate();
}

test "entry: CommandPayload serialize/deserialize" {
    const cmd = CommandPayload{
        .namespace_hash = 0xABCD_1234,
        .key_length = 5,
        .value_length = 3,
        .key = "hello",
        .value = "bye",
    };

    var buf: [128]u8 = undefined;
    const written = cmd.serialize(&buf);
    try std.testing.expect(written != null);
    try std.testing.expectEqual(@as(usize, COMMAND_PREFIX_SIZE + 5 + 3), written.?);

    const parsed = CommandPayload.deserialize(buf[0..written.?]);
    try std.testing.expect(parsed != null);

    const p = parsed.?;
    try std.testing.expectEqual(@as(u32, 0xABCD_1234), p.namespace_hash);
    try std.testing.expectEqual(@as(u16, 5), p.key_length);
    try std.testing.expectEqual(@as(u32, 3), p.value_length);
    try std.testing.expectEqualStrings("hello", p.key);
    try std.testing.expectEqualStrings("bye", p.value);
}

test "entry: buildCommandEntry end-to-end" {
    var payload_buf: [256]u8 = undefined;

    const entry = buildCommandEntry(
        .kv_put,
        Flags.NONE,
        2,
        50,
        9999,
        0x12345678,
        "mykey",
        "myvalue",
        &payload_buf,
    );
    try std.testing.expect(entry != null);

    const e = entry.?;
    try e.validate();

    // Extract command payload
    const cmd = e.commandPayload();
    try std.testing.expect(cmd != null);
    try std.testing.expectEqual(@as(u32, 0x12345678), cmd.?.namespace_hash);
    try std.testing.expectEqualStrings("mykey", cmd.?.key);
    try std.testing.expectEqualStrings("myvalue", cmd.?.value);
}

test "entry: empty payload (raft_noop)" {
    const entry = buildEntry(.raft_noop, Flags.NONE, 5, 1, 0, &[_]u8{});
    try entry.validate();
    try std.testing.expectEqual(@as(u32, 0), entry.header.payload_len);

    // Should not parse as command
    try std.testing.expectEqual(@as(?CommandPayload, null), entry.commandPayload());
}

test "entry: EntryType hasKeyValue" {
    try std.testing.expect(EntryType.kv_put.hasKeyValue());
    try std.testing.expect(EntryType.stream_append.hasKeyValue());
    try std.testing.expect(EntryType.queue_enqueue.hasKeyValue());
    try std.testing.expect(EntryType.ts_write.hasKeyValue());
    try std.testing.expect(!EntryType.raft_noop.hasKeyValue());
    try std.testing.expect(!EntryType.raft_config.hasKeyValue());
    try std.testing.expect(!EntryType.checkpoint.hasKeyValue());
}

test "entry: deserialize rejects truncated data" {
    // Too short for header
    try std.testing.expectEqual(@as(?Entry, null), Entry.deserialize("short"));

    // Header OK but payload truncated
    const payload = "test data";
    const entry = buildEntry(.kv_put, Flags.NONE, 1, 1, 0, payload);
    var buf: [256]u8 = undefined;
    const written = entry.serialize(&buf).?;
    // Truncate the last byte of payload
    try std.testing.expectEqual(@as(?Entry, null), Entry.deserialize(buf[0 .. written - 1]));
}

test "entry: CRC changes when payload changes" {
    const entry1 = buildEntry(.kv_put, Flags.NONE, 1, 1, 0, "payload_a");
    const entry2 = buildEntry(.kv_put, Flags.NONE, 1, 1, 0, "payload_b");
    try std.testing.expect(entry1.header.crc32c != entry2.header.crc32c);
}

test "entry: CRC changes when header fields change" {
    const payload = "same payload";
    const entry1 = buildEntry(.kv_put, Flags.NONE, 1, 1, 0, payload);
    const entry2 = buildEntry(.kv_put, Flags.NONE, 2, 1, 0, payload); // different term
    try std.testing.expect(entry1.header.crc32c != entry2.header.crc32c);
}
