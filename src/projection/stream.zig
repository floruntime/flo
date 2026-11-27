//! Streams as reads see them, and the shapes the streams machine
//! (`stream/machine.zig`) and its readers share: a stream's key, its ids,
//! an append's place in the log, the append value and batch codecs.
//!
//! Each stream has its own StreamID space (timestamp-sequence, like Redis).
//! Consumer groups are the group machine's (`stream/group_machine.zig`),
//! which reads a stream's records through `contract/streams.zig`.

const std = @import("std");
const NamespaceId = @import("../namespace/table.zig").Id;
const Allocator = std.mem.Allocator;
const entry_mod = @import("../storage/ual/entry.zig");
const router_mod = @import("router.zig");
const stream_id_mod = @import("../stream/stream_id.zig");
const node_router = @import("../node/router.zig");
const MAX_QUALIFIED_KEY = @import("../protocol/limits.zig").MAX_QUALIFIED_KEY;
const ce = @import("../protocol/channel_entries.zig");
const schema = @import("../protocol/schema.zig");
const client_names = @import("../protocol/names.zig");
const store_mod = @import("../contract/store.zig");
const view_mod = @import("../contract/view.zig");
const sm = @import("../stream/machine.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const CommandPayload = entry_mod.CommandPayload;
pub const StreamID = stream_id_mod.StreamID;

/// A stream's key in `stream.meta` and the prefix of its `stream.record`
/// keys: its namespace's id, then
/// its name's hash (`router.nsNameKey`). Leading with the id, two namespaces'
/// streams never share state, even when their names' hashes collide.
pub const StreamKey = [12]u8;

pub fn streamKey(ns: NamespaceId, name: []const u8) StreamKey {
    return node_router.nsNameKey(ns, name);
}

/// The longest name key: an id, then a stream's name.
pub const MAX_NAME_KEY = 4 + MAX_QUALIFIED_KEY;

/// A stream's name and settings key: its namespace's id, then its name.
/// The id is fixed width, so one namespace's names never prefix another's,
/// and a namespace's streams are the keys with its id as their first four
/// bytes. Null for a name too long to be one.
pub fn nameKey(buf: *[MAX_NAME_KEY]u8, ns: NamespaceId, name: []const u8) ?[]const u8 {
    if (name.len > MAX_QUALIFIED_KEY) return null;
    buf[0..4].* = ns.bytes();
    @memcpy(buf[4..][0..name.len], name);
    return buf[0 .. 4 + name.len];
}

/// The namespace a name key is in.
pub fn keyNamespace(key: []const u8) NamespaceId {
    return .of(std.mem.readInt(u32, key[0..4], .big));
}

/// What a trim removes: every record up to a boundary id, or every record
/// older than an age. An age is resolved where the trim applies, against
/// its entry's stamp, so the cut is the log's and the same on every replica.
pub const TrimBound = union(enum) {
    id: StreamID,
    age_ms: u64,

    const BY_ID: u8 = 0;
    const BY_AGE: u8 = 1;
    pub const MAX_SIZE = 17;

    /// The boundary id at `stamp_ns`, or null when nothing is that old (an
    /// age past the stamp trims nothing, never everything).
    pub fn resolve(self: TrimBound, stamp_ns: u64) ?StreamID {
        return switch (self) {
            .id => |id| id,
            .age_ms => |age| {
                const cutoff_ms = (stamp_ns / std.time.ns_per_ms) -| age;
                if (cutoff_ms == 0) return null;
                // Strictly older than the cutoff.
                return StreamID.fromTimestampMax(cutoff_ms - 1);
            },
        };
    }

    /// `[kind:u8]` then `[ts:u64][seq:u64]` for an id or `[age_ms:u64]`.
    pub fn encode(self: TrimBound, buf: *[MAX_SIZE]u8) []const u8 {
        switch (self) {
            .id => |id| {
                buf[0] = BY_ID;
                std.mem.writeInt(u64, buf[1..9], id.timestamp_ms, .little);
                std.mem.writeInt(u64, buf[9..17], id.sequence, .little);
                return buf[0..17];
            },
            .age_ms => |age| {
                buf[0] = BY_AGE;
                std.mem.writeInt(u64, buf[1..9], age, .little);
                return buf[0..9];
            },
        }
    }

    pub fn decode(key: []const u8) ?TrimBound {
        if (key.len == 17 and key[0] == BY_ID) return .{ .id = .{
            .timestamp_ms = std.mem.readInt(u64, key[1..9], .little),
            .sequence = std.mem.readInt(u64, key[9..17], .little),
        } };
        if (key.len == 9 and key[0] == BY_AGE) return .{ .age_ms = std.mem.readInt(u64, key[1..9], .little) };
        return null;
    }
};
pub const StreamIdGenerator = stream_id_mod.StreamIdGenerator;

// ═══════════════════════════════════════════════════════════════════════════════
// Per-Stream Record — maps StreamID → UAL index
// ═══════════════════════════════════════════════════════════════════════════════

pub const StreamRecord = struct {
    /// The StreamID assigned to this record. For a batch this is the *first*
    /// ID; the batch occupies `record_count` consecutive sequences from here.
    id: StreamID,
    /// UAL index of the entry the record's data lives in.
    ual_index: u64,
    /// Where in that entry: 0 for a `stream_append` itself, `m + 1` for
    /// message `m` of a `channel_inbound` (`appendAt` resolves either).
    part: u8 = 0,
    /// User partition index (for multi-partition streams).
    partition_index: u32,
    /// Number of logical records packed into this entry's batch blob. One
    /// entry may carry N user records, so this — not `records.items.len`,
    /// which counts entries — is what user-visible record counts must sum.
    record_count: u32 = 1,
    /// Stored payload size of this entry, in bytes (the batch blob, without
    /// the partition prefix). Summed for the reported stream size.
    byte_len: u32 = 0,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Stream-append value codec
// ═══════════════════════════════════════════════════════════════════════════════

/// Fixed `partition_index` prefix (LE u32) carried at the front of every
/// `stream_append` entry value, ahead of the user payload (the batch blob).
///
/// The partition is resolved at append time from request options
/// (`.partition` / `.partition_key`) and is not otherwise recoverable, so it
/// must ride inside the durable entry to survive restart/replay. Without it,
/// replay rebuilds every record as partition 0 and partition-filtered reads
/// break after a restart. Encode at every persist site; decode at
/// read/replay via `decodeAppendValue`.
pub const APPEND_PARTITION_PREFIX: usize = 4;

/// Decoded view of a stored stream-append value.
pub const AppendValue = struct {
    partition_index: u32,
    /// Inner payload (the batch blob), with the partition prefix stripped.
    payload: []const u8,
};

/// Prepend `partition_index` to `payload`, producing the value persisted in a
/// `stream_append` UAL entry. Caller owns the returned buffer.
pub fn encodeAppendValue(allocator: Allocator, partition_index: u32, payload: []const u8) ![]u8 {
    const buf = try allocator.alloc(u8, APPEND_PARTITION_PREFIX + payload.len);
    std.mem.writeInt(u32, buf[0..4], partition_index, .little);
    if (payload.len > 0) @memcpy(buf[APPEND_PARTITION_PREFIX..], payload);
    return buf;
}

/// The stored append a record locates, from its entry's payload and its
/// `part`: the one way any reader takes a record out of the log. Null for
/// a payload that isn't what the locator says.
pub fn appendAt(payload: []const u8, part: u8) ?AppendValue {
    const bytes = if (part == 0) payload else blk: {
        var diag: schema.Diagnostic = .{};
        const inbound = ce.Inbound.decodeAll(payload, &diag) catch return null;
        var it = inbound.messages.iterator();
        var i: usize = 0;
        while (it.next() catch return null) |m| : (i += 1) {
            if (i + 1 != part) continue;
            if (m.entry_type != @backingInt(EntryType.stream_append)) return null;
            break :blk m.payload;
        }
        return null;
    };
    const cmd = entry_mod.CommandPayload.deserialize(bytes) orelse return null;
    return decodeAppendValue(cmd.value);
}

/// Why a `stream_append` payload a channel message carries won't apply, or
/// null: one that isn't a command (its applier would halt,
/// `stream/handler.zig` `replayEntry`), or whose batch a client's append
/// would have been refused for. Checked before its inbound commits.
pub fn appendRefusal(payload: []const u8) ?[]const u8 {
    const cmd = entry_mod.CommandPayload.deserialize(payload) orelse return "bad request: a stream append that isn't one";
    // As a client's append is refused for its name: none, one with a
    // control character, or one longer than a stream list can name.
    if (cmd.key.len == 0) return "stream append: stream name is required";
    if (client_names.hasControl(cmd.key)) return comptime client_names.controlRefusal("a stream name");
    if (cmd.key.len > MAX_QUALIFIED_KEY) return "stream append: stream name too long";
    _ = BatchIterator.init(decodeAppendValue(cmd.value).payload) catch |err| return switch (err) {
        error.TooManyRecords => "stream append: a batch over the record limit",
        error.Malformed => "stream append: malformed batch",
    };
    return null;
}

/// An append value as a client's is sent: partition 0, then a batch of `n`
/// records of `payload_len` bytes each, with no headers. Its stored size
/// is `4 + n * (payload_len + 6)`.
pub fn appendValueOf(buf: []u8, n: u32, payload_len: u32) []const u8 {
    std.mem.writeInt(u32, buf[0..4], 0, .little);
    std.mem.writeInt(u32, buf[4..8], n, .little);
    var pos: usize = 8;
    for (0..n) |_| {
        std.mem.writeInt(u32, buf[pos..][0..4], payload_len, .little);
        pos += 4;
        @memset(buf[pos..][0..payload_len], 'x');
        pos += payload_len;
        std.mem.writeInt(u16, buf[pos..][0..2], 0, .little);
        pos += 2;
    }
    return buf[0..pos];
}

/// A record's `part` for message `ordinal` of an inbound.
pub fn partOf(ordinal: u8) u8 {
    comptime std.debug.assert(ce.MAX_BATCH <= std.math.maxInt(u8));
    std.debug.assert(ordinal < ce.MAX_BATCH);
    return ordinal + 1;
}

/// Split a stored stream-append value into its partition index and inner
/// payload. An empty append carries exactly the 4-byte prefix; a value shorter
/// than the prefix (malformed) decodes as partition 0 with the
/// bytes passed through unchanged.
pub fn decodeAppendValue(value: []const u8) AppendValue {
    if (value.len < APPEND_PARTITION_PREFIX) return .{ .partition_index = 0, .payload = value };
    return .{
        .partition_index = std.mem.readInt(u32, value[0..4], .little),
        .payload = value[APPEND_PARTITION_PREFIX..],
    };
}

/// Most records one append may carry. Reads return whole appends, so this is
/// also the most a single append adds to one read.
pub const MAX_BATCH_RECORDS: u32 = proto_limits.MAX_STREAM_BATCH_RECORDS;
const proto_limits = @import("../protocol/proto.zig");

/// Most records one read returns (whole appends, at least one).
pub const MAX_READ_RECORDS: u32 = proto_limits.MAX_STREAM_READ_RECORDS;

/// One record of a batch blob: its payload and its stored header bytes,
/// `([key_len:u16][key][val_len:u16][val])*`.
pub const BatchRecord = struct {
    payload: []const u8,
    headers_raw: []const u8,
    header_count: u16,
};

/// The records of a batch blob:
/// `[count:u32]([payload_len:u32][payload][header_count:u16]([klen:u16][k][vlen:u16][v])*)*`.
/// `init` checks the whole blob, so `next` never meets a malformed record.
pub const BatchIterator = struct {
    blob: []const u8,
    pos: usize = 4,
    left: u32,
    count: u32,

    pub const Error = error{ Malformed, TooManyRecords };

    pub fn init(blob: []const u8) Error!BatchIterator {
        if (blob.len < 4) return error.Malformed;
        const count = std.mem.readInt(u32, blob[0..4], .little);
        if (count == 0) return error.Malformed;
        // Each record takes at least 6 bytes; a count the blob can't hold
        // means it isn't a batch at all.
        if (count > (blob.len - 4) / 6) return error.Malformed;
        if (count > MAX_BATCH_RECORDS) return error.TooManyRecords;
        const it: BatchIterator = .{ .blob = blob, .left = count, .count = count };
        var walk = it;
        while (walk.left > 0) : (walk.left -= 1) {
            _ = walk.step() orelse return error.Malformed;
        }
        if (walk.pos != blob.len) return error.Malformed;
        return it;
    }

    /// The record count a blob claims, without checking the rest; for
    /// naming it in a refusal.
    pub fn claimedCount(blob: []const u8) u32 {
        if (blob.len < 4) return 0;
        return std.mem.readInt(u32, blob[0..4], .little);
    }

    pub fn next(self: *BatchIterator) ?BatchRecord {
        if (self.left == 0) return null;
        self.left -= 1;
        return self.step().?;
    }

    fn step(self: *BatchIterator) ?BatchRecord {
        const b = self.blob;
        var pos = self.pos;
        if (b.len - pos < 4) return null;
        const payload_len = std.mem.readInt(u32, b[pos..][0..4], .little);
        pos += 4;
        if (b.len - pos < payload_len) return null;
        const payload = b[pos .. pos + payload_len];
        pos += payload_len;
        if (b.len - pos < 2) return null;
        const header_count = std.mem.readInt(u16, b[pos..][0..2], .little);
        pos += 2;
        const headers_start = pos;
        var h: u16 = 0;
        while (h < header_count) : (h += 1) {
            inline for (0..2) |_| {
                if (b.len - pos < 2) return null;
                const len = std.mem.readInt(u16, b[pos..][0..2], .little);
                pos += 2;
                if (b.len - pos < len) return null;
                pos += len;
            }
        }
        self.pos = pos;
        return .{ .payload = payload, .headers_raw = b[headers_start..pos], .header_count = header_count };
    }
};

/// The id of an entry's last record: a cursor past the whole append.
pub fn lastRecordOf(rec: StreamRecord) StreamID {
    return .{ .timestamp_ms = rec.id.timestamp_ms, .sequence = rec.id.sequence + rec.record_count - 1 };
}

/// An upper bound on an append's bytes in a read answer. A record's answer
/// framing is 30 bytes against the 6 its stored form has (payload length,
/// header count), so 24 more; widening its header lengths from 2 to 4 bytes
/// at most doubles its stored header bytes. Twice the stored size covers the
/// headers, and 28 per record the framing.
pub fn answerBound(rec: StreamRecord) usize {
    return 2 * @as(usize, rec.byte_len) + 28 * @as(usize, rec.record_count);
}

/// How many of `records` (whole appends, in order) fit a budget of `limit`
/// logical records and one answer frame. The first always counts, so a read
/// makes progress even when one append holds more than `limit`; one append
/// always fits a frame (checked where appends are bounded).
pub fn wholeEntriesWithin(records: []const StreamRecord, limit: usize) usize {
    var total: usize = 0;
    var bytes: usize = 4; // the record count
    for (records, 0..) |rec, i| {
        total += rec.record_count;
        bytes += answerBound(rec);
        if (i > 0 and (total > limit or bytes > proto_limits.MAX_ANSWER_BYTES)) return i;
    }
    return records.len;
}

/// Number of logical records packed into a batch blob. Every append is stored
/// batch-wrapped as `[count:u32][...]`, so the count is a fixed-offset read —
/// no payload scan and no UAL re-read. Malformed/empty blobs count as one.
pub fn batchRecordCount(batch: []const u8) u32 {
    if (batch.len < 4) return 1;
    const n = std.mem.readInt(u32, batch[0..4], .little);
    return if (n == 0) 1 else n;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Stream Projection
// ═══════════════════════════════════════════════════════════════════════════════

/// A shard's streams as reads see them: the streams machine's families
/// (`stream/machine.zig`), as committed. Nothing here changes a stream; an
/// append, trim or delete is the machine's, applied from the log.
pub const StreamProjection = struct {
    /// The partition's store, wired once the partition is at its address.
    store: ?*store_mod.Store = null,

    stats: Stats = .{},

    pub const Stats = struct {
        reads: u64 = 0,
    };

    pub fn init() StreamProjection {
        return .{};
    }

    fn read(self: *const StreamProjection) ?view_mod.ReadView {
        return .{ .store = self.store orelse return null };
    }

    fn metaOf(self: *const StreamProjection, key: StreamKey) ?sm.Meta.Value {
        const m = sm.metaAt(self.read() orelse return null, key) orelse return null;
        return if (m.exists) m else null;
    }

    fn appends(self: *const StreamProjection, key: StreamKey, after_id: StreamID) ?sm.Appends {
        const m = self.metaOf(key) orelse return null;
        return sm.Appends.init(self.read().?, key, m, after_id);
    }

    /// Records with IDs in (after_id, to_id]: the start is a cursor,
    /// exclusive like `readStreamAfter`, the end inclusive. Appends are
    /// whole: one starting at or before `to_id` is returned entire.
    pub fn readStreamRange(self: *StreamProjection, key: StreamKey, after_id: StreamID, to_id: StreamID, filter_partition: ?u32, buf: []StreamRecord) usize {
        var it = self.appends(key, after_id) orelse return 0;
        var n: usize = 0;
        while (n < buf.len) {
            const rec = it.next() orelse break;
            if (rec.id.greaterThan(to_id)) break;
            if (filter_partition) |fp| if (rec.partition_index != fp) continue;
            buf[n] = rec;
            n += 1;
        }
        self.stats.reads += n;
        return n;
    }

    /// The appends holding records after `after_id` (exclusive): the one
    /// holding `after_id` first, when it holds more past it.
    pub fn readStreamAfter(self: *StreamProjection, key: StreamKey, after_id: StreamID, filter_partition: ?u32, buf: []StreamRecord) usize {
        return self.readStreamRange(key, after_id, StreamID.MAX, filter_partition, buf);
    }

    /// The appends whose first ids are `ids`, in that order; one the stream
    /// no longer holds, or never did, is skipped.
    pub fn readStreamByIds(self: *StreamProjection, key: StreamKey, ids: []const StreamID, buf: []StreamRecord) usize {
        const m = self.metaOf(key) orelse return 0;
        var n: usize = 0;
        for (ids) |id| {
            if (n >= buf.len) break;
            const rec = sm.appendOf(self.read().?, key, m, id) orelse continue;
            if (!rec.id.eql(id)) continue;
            buf[n] = rec;
            n += 1;
        }
        self.stats.reads += n;
        return n;
    }

    fn streams(self: *const StreamProjection) ?@import("../contract/streams.zig").Streams {
        return .{ .read = self.read() orelse return null };
    }

    /// The stream's last record, or MIN when it holds none.
    pub fn streamLastId(self: *const StreamProjection, key: StreamKey) StreamID {
        return if (self.streams()) |s| s.last(key) else StreamID.MIN;
    }

    /// The stream's first record, or MIN when it holds none.
    pub fn streamFirstId(self: *const StreamProjection, key: StreamKey) StreamID {
        return if (self.streams()) |s| s.first(key) else StreamID.MIN;
    }

    /// The last record a trim cut (the floor), or MIN.
    pub fn streamRemovedThrough(self: *const StreamProjection, key: StreamKey) StreamID {
        return if (self.streams()) |s| s.removedThrough(key) else StreamID.MIN;
    }

    /// The append holding a stream's record `id`, if it still does.
    pub fn streamAppendOf(self: *const StreamProjection, key: StreamKey, id: StreamID) ?StreamRecord {
        const m = self.metaOf(key) orelse return null;
        return sm.appendOf(self.read().?, key, m, id);
    }

    /// The ids of a stream's records after `after_id`, one per record.
    pub fn streamIdsAfter(self: *const StreamProjection, key: StreamKey, after_id: StreamID, out: []StreamID) usize {
        return if (self.streams()) |s| s.idsAfter(key, after_id, out) else 0;
    }

    /// Whether the stream holds any record above its floor.
    pub fn streamHolds(self: *const StreamProjection, key: StreamKey) bool {
        return self.streamLogicalCount(key) > 0;
    }

    /// Logical record count above the floor, each append's contents counted:
    /// what `stream read` returns and `stream info` reports.
    pub fn streamLogicalCount(self: *const StreamProjection, key: StreamKey) u64 {
        const m = self.metaOf(key) orelse return 0;
        return sm.countAbove(self.read().?, key, m, sm.floorOf(m)).records;
    }

    /// Stored size of a stream's records above its floor, in bytes.
    pub fn streamByteSize(self: *const StreamProjection, key: StreamKey) u64 {
        const m = self.metaOf(key) orelse return 0;
        return sm.countAbove(self.read().?, key, m, sm.floorOf(m)).bytes;
    }

    /// The floor a trim to `up_to_id` would leave: the last record of the
    /// last append whose first id is at or before it, never below today's.
    fn floorAfterTrim(self: *const StreamProjection, key: StreamKey, m: sm.Meta.Value, up_to_id: StreamID) StreamID {
        const floor = sm.floorOf(m);
        const rec = sm.appendOf(self.read().?, key, m, up_to_id) orelse {
            // No held append holds `up_to_id`: the last one before it.
            var it = self.read().?.rangeBefore(sm.Records, &key, blk: {
                var kb: [sm.RECORD_KEY_LEN]u8 = undefined;
                break :blk sm.recordKey(&kb, key, up_to_id);
            });
            const item = (it.next() catch return floor) orelse return floor;
            const last = lastRecordOf(sm.recordOf(item.key, item.value.v));
            return if (last.greaterThan(floor)) last else floor;
        };
        const last = lastRecordOf(rec);
        return if (last.greaterThan(floor)) last else floor;
    }

    /// The logical records a trim to `up_to_id` would remove, removing none.
    pub fn trimCount(self: *const StreamProjection, key: StreamKey, up_to_id: StreamID) u64 {
        const m = self.metaOf(key) orelse return 0;
        const r = self.read().?;
        const now = sm.countAbove(r, key, m, sm.floorOf(m)).records;
        return now - sm.countAbove(r, key, m, self.floorAfterTrim(key, m, up_to_id)).records;
    }

    /// The first id left after a trim to `up_to_id`; MIN when none would be.
    pub fn firstIdAfterTrim(self: *const StreamProjection, key: StreamKey, up_to_id: StreamID) StreamID {
        const m = self.metaOf(key) orelse return StreamID.MIN;
        var it = sm.Appends.init(self.read().?, key, m, self.floorAfterTrim(key, m, up_to_id));
        return if (it.next()) |rec| rec.id else StreamID.MIN;
    }

    /// The id a "keep the newest N" trim cuts through, for `count` records to
    /// go (1-indexed): the last append boundary that drops no more than
    /// `count`, so retention never deletes a record inside its window.
    /// MIN when even the first append would overshoot. Walks the appends a
    /// trim would cut: a leader's read, never an apply.
    pub fn resolveNthRecordId(self: *const StreamProjection, key: StreamKey, count: u64) StreamID {
        var it = self.appends(key, StreamID.MIN) orelse return StreamID.MIN;
        if (count == 0) return StreamID.MIN;
        var cum: u64 = 0;
        var cut = StreamID.MIN;
        while (it.next()) |rec| {
            if (cum + rec.record_count > count) break;
            cum += rec.record_count;
            cut = lastRecordOf(rec);
        }
        return cut;
    }

    /// Bytes the streams' records hold in the store.
    pub fn memoryUsage(self: *const StreamProjection) usize {
        const st = self.store orelse return @sizeOf(StreamProjection);
        return @sizeOf(StreamProjection) + st.family(sm.Records.ID).encoded_bytes;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn sid(ms: u64, seq: u64) StreamID {
    return .{ .timestamp_ms = ms, .sequence = seq };
}

fn testProjection(t: *sm.TestStreams) StreamProjection {
    return .{ .store = &t.store };
}

test "stream: same-named streams in two namespaces share no state, and a stream's hash names its key" {
    const one: NamespaceId = .of(2);
    const two: NamespaceId = .of(3);
    const a = streamKey(one, "orders");
    const b = streamKey(two, "orders");
    try testing.expect(!std.mem.eql(u8, &a, &b));
    try testing.expectEqualSlices(u8, a[4..12], b[4..12]);
}

test "stream: reads go by the stream's appends above its floor, a range whole appends at or before its end, by ids exactly" {
    var t = sm.TestStreams.init();
    defer t.deinit();
    var p = testProjection(&t);
    const k = sm.TestStreams.sk("s");
    _ = try t.append("s", 100, 3, 4);
    _ = try t.append("s", 101, 2, 4);
    _ = try t.append("s", 102, 1, 4);
    var buf: [8]StreamRecord = undefined;
    try testing.expectEqual(@as(usize, 3), p.readStreamAfter(k, StreamID.MIN, null, &buf));
    // A cursor inside the first append resumes in it.
    try testing.expectEqual(@as(usize, 3), p.readStreamAfter(k, sid(100, 1), null, &buf));
    try testing.expectEqual(@as(usize, 2), p.readStreamAfter(k, sid(100, 2), null, &buf));
    try testing.expectEqual(@as(usize, 2), p.readStreamRange(k, StreamID.MIN, sid(101, 0), null, &buf));
    const ids = [_]StreamID{ sid(102, 0), sid(100, 1), sid(100, 0) };
    try testing.expectEqual(@as(usize, 2), p.readStreamByIds(k, &ids, &buf));
    try testing.expect(buf[0].id.eql(sid(102, 0)) and buf[1].id.eql(sid(100, 0)));
    try testing.expectEqual(@as(u64, 6), p.streamLogicalCount(k));
    try testing.expectEqual(@as(u64, 72), p.streamByteSize(k));
    // A trim through the first append: it reads as gone at once.
    _ = try t.trim("s", .{ .id = sid(100, 0) });
    try testing.expectEqual(@as(usize, 2), p.readStreamAfter(k, StreamID.MIN, null, &buf));
    try testing.expectEqual(@as(usize, 0), p.readStreamByIds(k, ids[2..], &buf));
    try testing.expectEqual(@as(u64, 3), p.streamLogicalCount(k));
    try testing.expect(p.streamFirstId(k).eql(sid(101, 0)));
    try testing.expect(p.streamLastId(k).eql(sid(102, 0)));
}

test "stream: a trim's count and the first id it leaves are predicted without trimming, by records not appends" {
    var t = sm.TestStreams.init();
    defer t.deinit();
    var p = testProjection(&t);
    const k = sm.TestStreams.sk("s");
    _ = try t.append("s", 100, 3, 4);
    _ = try t.append("s", 101, 2, 4);
    try testing.expectEqual(@as(u64, 3), p.trimCount(k, sid(100, 1)));
    try testing.expect(p.firstIdAfterTrim(k, sid(100, 1)).eql(sid(101, 0)));
    try testing.expectEqual(@as(u64, 5), p.trimCount(k, sid(101, 0)));
    try testing.expect(p.firstIdAfterTrim(k, sid(101, 0)).eql(StreamID.MIN));
    try testing.expectEqual(@as(u64, 0), p.trimCount(k, sid(99, 0)));
    try testing.expectEqual(@as(u64, 5), p.streamLogicalCount(k));
}

test "stream: count retention resolves by records, not appends, and never cuts inside its window" {
    var t = sm.TestStreams.init();
    defer t.deinit();
    var p = testProjection(&t);
    const k = sm.TestStreams.sk("s");
    for (0..4) |i| _ = try t.append("s", 100 + i, 3, 4);
    // Keep 6 of 12: 6 go, exactly two appends.
    try testing.expect(p.resolveNthRecordId(k, 6).eql(sid(101, 2)));
    // 5 to go: one append (3); a second would overshoot.
    try testing.expect(p.resolveNthRecordId(k, 5).eql(sid(100, 2)));
    try testing.expect(p.resolveNthRecordId(k, 2).eql(StreamID.MIN));
}

fn testBatch(buf: []u8, payloads: []const []const u8) []const u8 {
    std.mem.writeInt(u32, buf[0..4], @intCast(payloads.len), .little);
    var pos: usize = 4;
    for (payloads) |p| {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(p.len), .little);
        pos += 4;
        @memcpy(buf[pos .. pos + p.len], p);
        pos += p.len;
        std.mem.writeInt(u16, buf[pos..][0..2], 0, .little);
        pos += 2;
    }
    return buf[0..pos];
}

test "BatchIterator: yields every record with its headers" {
    // Two records; the second carries one header k=v.
    const blob = "\x02\x00\x00\x00" ++ "\x01\x00\x00\x00a\x00\x00" ++ "\x02\x00\x00\x00bc\x01\x00\x01\x00k\x01\x00v";
    var it = try BatchIterator.init(blob);
    try testing.expectEqual(@as(u32, 2), it.count);
    const r1 = it.next().?;
    try testing.expectEqualStrings("a", r1.payload);
    try testing.expectEqual(@as(u16, 0), r1.header_count);
    const r2 = it.next().?;
    try testing.expectEqualStrings("bc", r2.payload);
    try testing.expectEqual(@as(u16, 1), r2.header_count);
    try testing.expectEqualStrings("\x01\x00k\x01\x00v", r2.headers_raw);
    try testing.expectEqual(@as(?BatchRecord, null), it.next());
}

test "BatchIterator: refuses malformed blobs and too many records" {
    const bad = [_][]const u8{
        "",
        "\x01\x00\x00",
        "\x00\x00\x00\x00", // no records
        "\x01\x00\x00\x00\x05\x00\x00\x00ab\x00\x00", // payload runs past the end
        "\x01\x00\x00\x00\x01\x00\x00\x00a\x00", // header count cut short
        "\x01\x00\x00\x00\x01\x00\x00\x00a\x01\x00\x09\x00k", // header key runs past the end
        "\x01\x00\x00\x00\x01\x00\x00\x00a\x00\x00X", // trailing byte
        "\x02\x00\x00\x00\x01\x00\x00\x00a\x00\x00", // claims two, holds one
        "a", // a raw payload, not a batch
    };
    for (bad) |b| try testing.expectError(error.Malformed, BatchIterator.init(b));

    const payloads = @as([(MAX_BATCH_RECORDS + 1)][]const u8, @splat("x"));
    var buf: [4 + (MAX_BATCH_RECORDS + 1) * 7]u8 = undefined;
    const over = testBatch(&buf, &payloads);
    try testing.expectError(error.TooManyRecords, BatchIterator.init(over));
    try testing.expectEqual(MAX_BATCH_RECORDS + 1, BatchIterator.claimedCount(over));
}

test "BatchIterator: accepts exactly the record limit" {
    const payloads = @as([MAX_BATCH_RECORDS][]const u8, @splat("x"));
    var buf: [4 + MAX_BATCH_RECORDS * 7]u8 = undefined;
    var it = try BatchIterator.init(testBatch(&buf, &payloads));
    var n: u32 = 0;
    while (it.next()) |_| n += 1;
    try testing.expectEqual(MAX_BATCH_RECORDS, n);
}

test "wholeEntriesWithin: whole appends within the record budget, at least one" {
    const rec = struct {
        fn of(n: u32) StreamRecord {
            return .{ .id = StreamID.MIN, .ual_index = 0, .partition_index = 0, .record_count = n };
        }
    }.of;
    const recs = [_]StreamRecord{ rec(400), rec(400), rec(400) };
    try testing.expectEqual(@as(usize, 1), wholeEntriesWithin(&recs, 10)); // first alone exceeds
    try testing.expectEqual(@as(usize, 1), wholeEntriesWithin(&recs, 799));
    try testing.expectEqual(@as(usize, 2), wholeEntriesWithin(&recs, 800));
    try testing.expectEqual(@as(usize, 3), wholeEntriesWithin(&recs, 1000 * 1000));
    try testing.expectEqual(@as(usize, 0), wholeEntriesWithin(recs[0..0], 5));
}
