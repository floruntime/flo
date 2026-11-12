//! Stream Projection — per-stream record tracking.
//!
//! Each stream has its own StreamID space (timestamp-sequence, like Redis).
//! Consumer groups are the group machine's (`stream/group_machine.zig`),
//! which reads a stream's records through `contract/streams.zig`.
//!
//! Data flow:
//!   stream_append → per-stream ID gen → record StreamID → UAL index mapping
//!   stream_trim   → remove records with IDs below threshold

const std = @import("std");
const NamespaceId = @import("../namespace/table.zig").Id;
const Allocator = std.mem.Allocator;
const entry_mod = @import("../storage/ual/entry.zig");
const router_mod = @import("router.zig");
const stream_id_mod = @import("../stream/stream_id.zig");
const node_router = @import("../node/router.zig");
const MAX_QUALIFIED_KEY = @import("../protocol/limits.zig").MAX_QUALIFIED_KEY;

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const CommandPayload = entry_mod.CommandPayload;
pub const StreamID = stream_id_mod.StreamID;

/// A stream's records, as the projection keys them: its namespace's id, then
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
    /// UAL index where the record's data lives.
    ual_index: u64,
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
// Per-Stream State
// ═══════════════════════════════════════════════════════════════════════════════

pub const StreamState = struct {
    /// Per-stream ID generator for monotonic StreamIDs.
    id_gen: StreamIdGenerator,
    /// Records sorted by StreamID (append-only, trimmed from front).
    records: std.ArrayList(StreamRecord),
    /// First record ID (or MIN if empty).
    first_id: StreamID,
    /// Last record ID (or MIN if empty).
    last_id: StreamID,
    /// The last record a trim removed, never lowered: a trim cuts whole
    /// appends, so this is the last record of the last append it cut, which
    /// can lie past the trim's own bound. MIN until a trim removes one.
    removed_through: StreamID,
    /// The stream's key in its projection.
    key: StreamKey,
    /// Running sum of `record_count` over live entries — the logical record
    /// count, maintained incrementally so `stream info` stays O(1).
    total_records: u64,
    /// Running sum of `byte_len` over live entries.
    total_bytes: u64,

    pub fn init(key: StreamKey) StreamState {
        return .{
            .id_gen = StreamIdGenerator.init(),
            .records = .empty,
            .first_id = StreamID.MIN,
            .last_id = StreamID.MIN,
            .removed_through = StreamID.MIN,
            .key = key,
            .total_records = 0,
            .total_bytes = 0,
        };
    }

    pub fn deinit(self: *StreamState, allocator: Allocator) void {
        self.records.deinit(allocator);
    }

    /// Append anchored to an explicit timestamp (the originating UAL entry's
    /// timestamp, in ms) so replay reproduces identical StreamIDs.
    pub fn appendAt(self: *StreamState, allocator: Allocator, ual_index: u64, partition_index: u32, ts_ms: u64, record_count: u32, byte_len: u32) !StreamID {
        // Reserve one sequence per logical record: reads expand a batch into
        // `id.sequence + 0..n-1`, so reserving a single sequence for an
        // N-record batch would let the next append in the same millisecond
        // hand out IDs the batch has already used.
        const n = @max(1, record_count);
        const batch = try self.id_gen.nextBatchAt(ts_ms, n);
        try self.records.append(allocator, .{
            .id = batch.first,
            .ual_index = ual_index,
            .partition_index = partition_index,
            .record_count = n,
            .byte_len = byte_len,
        });
        if (self.first_id.eql(StreamID.MIN)) {
            self.first_id = batch.first;
        }
        // The stream's last ID is the batch's *last* record, not its first.
        self.last_id = batch.last;
        self.total_records += n;
        self.total_bytes += byte_len;
        return batch.first;
    }

    /// Read records with IDs in (after_id, to_id]: the start is a cursor,
    /// exclusive like `readAfter`, the end inclusive.
    /// Entries are whole: one starting at or before `to_id` is returned
    /// entire, records past `to_id` included.
    pub fn readRange(self: *const StreamState, after_id: StreamID, to_id: StreamID, filter_partition: ?u32, buf: []StreamRecord) usize {
        const items = self.records.items;
        if (items.len == 0) return 0;
        const start_idx = self.firstAfter(after_id);
        if (start_idx >= items.len) return 0;
        var n: usize = 0;
        var i = start_idx;
        while (i < items.len and n < buf.len) : (i += 1) {
            const rec = &items[i];
            if (rec.id.greaterThan(to_id)) break;
            if (filter_partition) |fp| {
                if (rec.partition_index != fp) continue;
            }
            buf[n] = rec.*;
            n += 1;
        }
        return n;
    }

    /// The first entry holding a record after `after_id`. That can be the
    /// entry holding `after_id` itself, so a cursor inside an append resumes
    /// with the rest of it; readers skip its records up to the cursor.
    fn firstAfter(self: *const StreamState, after_id: StreamID) usize {
        const items = self.records.items;
        var i = self.lowerBound(after_id);
        if (i > 0 and lastRecordOf(items[i - 1]).greaterThan(after_id)) i -= 1;
        while (i < items.len and !lastRecordOf(items[i]).greaterThan(after_id)) : (i += 1) {}
        return i;
    }

    /// The ids of the records after `after_id`, one per record, oldest
    /// first: an append's records are its first id's sequence onwards.
    pub fn idsAfter(self: *const StreamState, after_id: StreamID, out: []StreamID) usize {
        const items = self.records.items;
        var n: usize = 0;
        var i = self.firstAfter(after_id);
        while (i < items.len and n < out.len) : (i += 1) {
            const rec = items[i];
            var k: u32 = 0;
            while (k < rec.record_count and n < out.len) : (k += 1) {
                const id: StreamID = .{ .timestamp_ms = rec.id.timestamp_ms, .sequence = rec.id.sequence + k };
                if (!id.greaterThan(after_id)) continue;
                out[n] = id;
                n += 1;
            }
        }
        return n;
    }

    /// The append holding record `id`, if the stream still does.
    pub fn appendOf(self: *const StreamState, id: StreamID) ?StreamRecord {
        const items = self.records.items;
        var i = self.lowerBound(id);
        if (i < items.len and items[i].id.eql(id)) return items[i];
        if (i == 0) return null;
        i -= 1;
        return if (lastRecordOf(items[i]).lessThan(id)) null else items[i];
    }

    /// Read the entries holding records after `after_id` (exclusive).
    pub fn readAfter(self: *const StreamState, after_id: StreamID, filter_partition: ?u32, buf: []StreamRecord) usize {
        const items = self.records.items;
        if (items.len == 0) return 0;
        const start_idx = self.firstAfter(after_id);
        if (start_idx >= items.len) return 0;
        var n: usize = 0;
        var i = start_idx;
        while (i < items.len and n < buf.len) : (i += 1) {
            if (filter_partition) |fp| {
                if (items[i].partition_index != fp) continue;
            }
            buf[n] = items[i];
            n += 1;
        }
        return n;
    }

    /// Resolve specific StreamIDs back to records. Output preserves the input
    /// order; any ID not currently in the stream (trimmed, never appended, or
    /// belonging to a different stream) is silently skipped. Returns the
    /// number of records written to `buf`.
    pub fn readByIds(self: *const StreamState, ids: []const StreamID, buf: []StreamRecord) usize {
        const items = self.records.items;
        if (items.len == 0) return 0;
        var n: usize = 0;
        for (ids) |id| {
            if (n >= buf.len) break;
            const idx = self.lowerBound(id);
            if (idx >= items.len) continue;
            if (!items[idx].id.eql(id)) continue;
            buf[n] = items[idx];
            n += 1;
        }
        return n;
    }

    /// Number of live append *entries* (batches). Internal bookkeeping —
    /// user-visible record counts must use `logicalCount`.
    pub fn count(self: *const StreamState) usize {
        return self.records.items.len;
    }

    /// Number of live logical records, counting each batch's contents.
    pub fn logicalCount(self: *const StreamState) u64 {
        return self.total_records;
    }

    /// Stored size of the live records, in bytes.
    pub fn byteSize(self: *const StreamState) u64 {
        return self.total_bytes;
    }

    /// What a trim to `up_to_id` would remove: the leading entries with
    /// id <= up_to_id, and the logical records they hold.
    pub fn trimExtent(self: *const StreamState, up_to_id: StreamID) struct { entries: usize, records: u64 } {
        const items = self.records.items;
        var cut: usize = 0;
        var records: u64 = 0;
        while (cut < items.len) : (cut += 1) {
            if (items[cut].id.greaterThan(up_to_id)) break;
            records += items[cut].record_count;
        }
        return .{ .entries = cut, .records = records };
    }

    /// Trim records with IDs <= up_to_id. Returns the logical records removed,
    /// the same number `trimExtent` predicts.
    pub fn trim(self: *StreamState, allocator: Allocator, up_to_id: StreamID) u64 {
        _ = allocator;
        const items = self.records.items;
        if (items.len == 0) return 0;

        const extent = self.trimExtent(up_to_id);
        const cut = extent.entries;

        if (cut == 0) return 0;
        const removed_last = lastRecordOf(items[cut - 1]);
        if (removed_last.greaterThan(self.removed_through)) self.removed_through = removed_last;

        // Drop the trimmed entries from the running totals before shifting.
        for (items[0..cut]) |rec| {
            self.total_records -= @min(self.total_records, rec.record_count);
            self.total_bytes -= @min(self.total_bytes, rec.byte_len);
        }

        // Shift remaining records to front
        const remaining = items.len - cut;
        if (remaining > 0) {
            std.mem.copyForwards(StreamRecord, items[0..remaining], items[cut..items.len]);
        }
        self.records.items.len = remaining;

        if (remaining > 0) {
            self.first_id = self.records.items[0].id;
        } else {
            self.first_id = StreamID.MIN;
            self.last_id = StreamID.MIN;
            self.total_records = 0;
            self.total_bytes = 0;
        }
        return extent.records;
    }

    /// Binary search: find first index where record.id >= target.
    fn lowerBound(self: *const StreamState, target: StreamID) usize {
        const items = self.records.items;
        var lo: usize = 0;
        var hi: usize = items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (items[mid].id.lessThan(target)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
};

/// Stream metadata — partition count, retention policy, and other per-stream config.
pub const StreamMetadata = struct {
    partition_count: u32 = 1,
    /// Retention: max age in seconds (0 = forever).
    retention_age_s: u64 = 0,
    /// Retention: max record count (0 = unlimited).
    retention_count: u64 = 0,
    /// Retention: max bytes (0 = unlimited).
    retention_bytes: u64 = 0,

    /// Returns true if any retention policy is configured.
    pub fn hasRetention(self: StreamMetadata) bool {
        return self.retention_age_s > 0 or self.retention_count > 0 or self.retention_bytes > 0;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Stream Projection
// ═══════════════════════════════════════════════════════════════════════════════

pub const StreamProjection = struct {
    allocator: Allocator,

    /// Registered streams, by `nameKey` (for listing).
    stream_names: std.StringHashMap(void),

    /// Stream metadata (partition count, etc.), by `nameKey`.
    stream_metadata: std.StringHashMap(StreamMetadata),

    /// Per-stream state — each stream has its own StreamID space.
    streams: std.AutoHashMap(StreamKey, StreamState),

    /// Stats.
    stats: Stats,

    pub const Stats = struct {
        appended: u64 = 0,
        reads: u64 = 0,
        trimmed: u64 = 0,
    };

    pub fn init(allocator: Allocator) StreamProjection {
        return .{
            .allocator = allocator,
            .stream_names = std.StringHashMap(void).init(allocator),
            .stream_metadata = std.StringHashMap(StreamMetadata).init(allocator),
            .streams = std.AutoHashMap(StreamKey, StreamState).init(allocator),
            .stats = .{},
        };
    }

    pub fn deinit(self: *StreamProjection) void {
        // Free per-stream state
        var sit = self.streams.iterator();
        while (sit.next()) |kv| {
            kv.value_ptr.deinit(self.allocator);
        }
        self.streams.deinit();
        // Free stream name keys
        var nit = self.stream_names.keyIterator();
        while (nit.next()) |key| {
            self.allocator.free(@constCast(key.*));
        }
        self.stream_names.deinit();
        // Free stream metadata keys
        var mit = self.stream_metadata.keyIterator();
        while (mit.next()) |key| {
            self.allocator.free(@constCast(key.*));
        }
        self.stream_metadata.deinit();
    }

    /// Reset the stream projection to empty state.
    /// Clears every stream on this shard, as a restart would.
    pub fn reset(self: *StreamProjection) void {
        // Free per-stream state
        var sit = self.streams.iterator();
        while (sit.next()) |kv| {
            kv.value_ptr.deinit(self.allocator);
        }
        self.streams.clearAndFree();
        // Free stream name keys
        var nit = self.stream_names.keyIterator();
        while (nit.next()) |key| {
            self.allocator.free(@constCast(key.*));
        }
        self.stream_names.clearAndFree();
        // Free stream metadata keys
        var mit = self.stream_metadata.keyIterator();
        while (mit.next()) |key| {
            self.allocator.free(@constCast(key.*));
        }
        self.stream_metadata.clearAndFree();
        self.stats = .{};
    }

    // ─── Per-stream state ──────────────────────────────────────────────────

    /// Get or create per-stream state for a given name hash.
    pub fn getOrCreateStream(self: *StreamProjection, key: StreamKey) !*StreamState {
        const gop = try self.streams.getOrPut(key);
        if (!gop.found_existing) {
            gop.value_ptr.* = StreamState.init(key);
        }
        return gop.value_ptr;
    }

    /// Append anchored to the originating UAL entry's timestamp (ms), so
    /// replay reproduces identical StreamIDs.
    pub fn appendToStreamAt(self: *StreamProjection, key: StreamKey, ual_index: u64, partition_index: u32, ts_ms: u64, record_count: u32, byte_len: u32) !StreamID {
        const ss = try self.getOrCreateStream(key);
        const id = try ss.appendAt(self.allocator, ual_index, partition_index, ts_ms, record_count, byte_len);
        self.stats.appended += 1;
        return id;
    }

    /// Read records from a stream with IDs in (after_id, to_id].
    pub fn readStreamRange(self: *StreamProjection, key: StreamKey, after_id: StreamID, to_id: StreamID, filter_partition: ?u32, buf: []StreamRecord) usize {
        const ss = self.streams.getPtr(key) orelse return 0;
        const n = ss.readRange(after_id, to_id, filter_partition, buf);
        self.stats.reads += n;
        return n;
    }

    /// Read records from a stream after a given ID (exclusive).
    pub fn readStreamAfter(self: *StreamProjection, key: StreamKey, after_id: StreamID, filter_partition: ?u32, buf: []StreamRecord) usize {
        const ss = self.streams.getPtr(key) orelse return 0;
        const n = ss.readAfter(after_id, filter_partition, buf);
        self.stats.reads += n;
        return n;
    }

    /// Resolve specific StreamIDs back to records (preserves input order, skips
    /// missing IDs).
    pub fn readStreamByIds(self: *StreamProjection, key: StreamKey, ids: []const StreamID, buf: []StreamRecord) usize {
        const ss = self.streams.getPtr(key) orelse return 0;
        const n = ss.readByIds(ids, buf);
        self.stats.reads += n;
        return n;
    }

    /// Get the last StreamID for a stream.
    pub fn streamLastId(self: *const StreamProjection, key: StreamKey) StreamID {
        const ss = self.streams.getPtr(key) orelse return StreamID.MIN;
        return ss.last_id;
    }

    /// Get the first StreamID for a stream.
    pub fn streamFirstId(self: *const StreamProjection, key: StreamKey) StreamID {
        const ss = self.streams.getPtr(key) orelse return StreamID.MIN;
        return ss.first_id;
    }

    /// The last record a trim removed from a stream (`StreamState.removed_through`).
    pub fn streamRemovedThrough(self: *const StreamProjection, key: StreamKey) StreamID {
        const ss = self.streams.getPtr(key) orelse return StreamID.MIN;
        return ss.removed_through;
    }

    /// The append holding a stream's record `id`, if it still does.
    pub fn streamAppendOf(self: *const StreamProjection, key: StreamKey, id: StreamID) ?StreamRecord {
        const ss = self.streams.getPtr(key) orelse return null;
        return ss.appendOf(id);
    }

    /// The ids of a stream's records after `after_id`, one per record.
    pub fn streamIdsAfter(self: *const StreamProjection, key: StreamKey, after_id: StreamID, out: []StreamID) usize {
        const ss = self.streams.getPtr(key) orelse return 0;
        return ss.idsAfter(after_id, out);
    }

    /// Get the record count for a stream.
    pub fn streamRecordCount(self: *const StreamProjection, key: StreamKey) usize {
        const ss = self.streams.getPtr(key) orelse return 0;
        return ss.count();
    }

    /// Logical record count — sums each append batch's contents. This is the
    /// number a user sees from `stream read`, and what `stream info` reports.
    pub fn streamLogicalCount(self: *const StreamProjection, key: StreamKey) u64 {
        const ss = self.streams.getPtr(key) orelse return 0;
        return ss.logicalCount();
    }

    /// Stored size of a stream's live records, in bytes.
    pub fn streamByteSize(self: *const StreamProjection, key: StreamKey) u64 {
        const ss = self.streams.getPtr(key) orelse return 0;
        return ss.byteSize();
    }

    /// Trim a stream — remove records with IDs <= up_to_id.
    pub fn trimStream(self: *StreamProjection, key: StreamKey, up_to_id: StreamID) u64 {
        const ss = self.streams.getPtr(key) orelse return 0;
        const trimmed = ss.trim(self.allocator, up_to_id);
        self.stats.trimmed += trimmed;
        return trimmed;
    }

    /// The logical records a trim to `up_to_id` would remove, removing none.
    pub fn trimCount(self: *const StreamProjection, key: StreamKey, up_to_id: StreamID) u64 {
        const ss = self.streams.getPtr(key) orelse return 0;
        return ss.trimExtent(up_to_id).records;
    }

    /// The first id left after a trim to `up_to_id`; MIN when none would be.
    pub fn firstIdAfterTrim(self: *const StreamProjection, key: StreamKey, up_to_id: StreamID) StreamID {
        const ss = self.streams.getPtr(key) orelse return StreamID.MIN;
        const cut = ss.trimExtent(up_to_id).entries;
        if (cut >= ss.records.items.len) return StreamID.MIN;
        return ss.records.items[cut].id;
    }

    /// Resolve the StreamID of the Nth record in a stream (1-indexed).
    /// Used by trim-by-count to convert a count to a deterministic StreamID
    /// before persisting through Raft.
    pub fn resolveNthRecordId(self: *const StreamProjection, key: StreamKey, count: u64) StreamID {
        const ss = self.streams.getPtr(key) orelse return StreamID.MIN;
        const records = ss.records.items;
        if (records.len == 0 or count == 0) return StreamID.MIN;
        // `count` is in logical records, so walk batch contents rather than
        // indexing entries — otherwise "keep N records" retention kept N
        // *batches* (N×batch_size records) for batched producers.
        //
        // Trim is entry-granular, so `count` rarely lands on a boundary. Cut at
        // the last boundary that drops NO MORE than `count` records: retention
        // is a cap, and keeping a few extra beats deleting records still inside
        // the window. Returns MIN (trim nothing) when even the first entry
        // would overshoot.
        var cum: u64 = 0;
        var cut = StreamID.MIN;
        for (records) |rec| {
            if (cum + rec.record_count > count) break;
            cum += rec.record_count;
            cut = .{
                .timestamp_ms = rec.id.timestamp_ms,
                .sequence = rec.id.sequence + rec.record_count - 1,
            };
        }
        return cut;
    }

    /// Delete a stream entirely: drop its records (by `key`) plus its name
    /// registry and metadata entries (by `name_key`, its `nameKey`). Mirror
    /// of `registerStream` / `trimStream`. Returns true if the stream existed.
    pub fn deleteStream(self: *StreamProjection, key: StreamKey, name_key: []const u8) bool {
        var existed = false;
        if (self.streams.fetchRemove(key)) |kv| {
            var ss = kv.value;
            ss.deinit(self.allocator);
            existed = true;
        }
        if (self.stream_names.fetchRemove(name_key)) |kv| {
            self.allocator.free(@constCast(kv.key));
            existed = true;
        }
        if (self.stream_metadata.fetchRemove(name_key)) |kv| {
            self.allocator.free(@constCast(kv.key));
            existed = true;
        }

        return existed;
    }

    // ─── Stream Name Registry ──────────────────────────────────────────────

    /// Register a stream by its `nameKey`. No-op if already registered.
    pub fn registerStream(self: *StreamProjection, name: []const u8) !void {
        if (name.len == 0) return;
        const gop = try self.stream_names.getOrPut(name);
        if (!gop.found_existing) {
            // The new slot's key is the caller's slice until the copy lands.
            errdefer self.stream_names.removeByPtr(gop.key_ptr);
            gop.key_ptr.* = try self.allocator.dupe(u8, name);
        }
    }

    /// Register (or update) metadata for a stream.
    pub fn registerStreamMetadata(self: *StreamProjection, name: []const u8, meta: StreamMetadata) !void {
        if (name.len == 0) return;
        const gop = try self.stream_metadata.getOrPut(name);
        if (!gop.found_existing) {
            // The new slot's key is the caller's slice until the copy lands.
            errdefer self.stream_metadata.removeByPtr(gop.key_ptr);
            gop.key_ptr.* = try self.allocator.dupe(u8, name);
        }
        gop.value_ptr.* = .{
            .partition_count = @max(1, meta.partition_count),
            .retention_age_s = meta.retention_age_s,
            .retention_count = meta.retention_count,
            .retention_bytes = meta.retention_bytes,
        };
    }

    /// Get the partition count for a stream (defaults to 1).
    pub fn getPartitionCount(self: *const StreamProjection, name: []const u8) u32 {
        if (self.stream_metadata.get(name)) |meta| return meta.partition_count;
        return 1;
    }

    /// Namespace `ns`'s stream names into a buffer, past the id that leads
    /// their keys. Returns the count written. Borrowed from the map (valid
    /// until it changes).
    pub fn scanStreamNames(self: *const StreamProjection, ns: NamespaceId, buf: [][]const u8) usize {
        const prefix = ns.bytes();
        var count: usize = 0;
        var it = self.stream_names.keyIterator();
        while (it.next()) |key| {
            if (count >= buf.len) break;
            if (!std.mem.startsWith(u8, key.*, &prefix)) continue;
            buf[count] = key.*[prefix.len..];
            count += 1;
        }
        return count;
    }

    /// Number of registered stream names.
    pub fn streamCount(self: *const StreamProjection) usize {
        return self.stream_names.count();
    }

    pub fn memoryUsage(self: *const StreamProjection) usize {
        var mem: usize = @sizeOf(StreamProjection);

        // Per-stream state
        var sit = self.streams.iterator();
        while (sit.next()) |kv| {
            mem += kv.value_ptr.records.items.len * @sizeOf(StreamRecord);
        }

        // Stream names
        var nit = self.stream_names.keyIterator();
        while (nit.next()) |key| {
            mem += key.len;
        }

        return mem;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;
/// The stamp test appends take: ids from a fixed time, as replay's are.
const TEST_MS: u64 = 1_700_000_000_000;

/// A stream key for a test that names its stream by a number.
fn testKey(n: u32) StreamKey {
    return streamKey(.default, std.mem.asBytes(&n));
}

test "stream: same-named streams in two namespaces share no state, even when their names' hashes are equal" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();
    const one: NamespaceId = .of(16);
    const two: NamespaceId = .of(17);
    // Different names, their hashes forced equal: only the id that leads
    // the key tells them apart.
    const a = streamKey(one, "orders");
    var b = streamKey(two, "other");
    b[4..12].* = a[4..12].*;
    try testing.expect(!std.mem.eql(u8, &a, &b));

    _ = try s.appendToStreamAt(a, 1, 0, TEST_MS, 3, 30);
    _ = try s.appendToStreamAt(b, 2, 0, TEST_MS, 1, 10);
    try testing.expectEqual(@as(u64, 3), s.streamLogicalCount(a));
    try testing.expectEqual(@as(u64, 1), s.streamLogicalCount(b));

    // The same stream name in both: two names.
    var na: [MAX_NAME_KEY]u8 = undefined;
    var nb: [MAX_NAME_KEY]u8 = undefined;
    try s.registerStream(nameKey(&na, one, "orders").?);
    try s.registerStream(nameKey(&nb, two, "orders").?);

    // Each namespace lists its own name, past the id.
    var names: [4][]const u8 = undefined;
    try testing.expectEqual(@as(usize, 1), s.scanStreamNames(one, &names));
    try testing.expectEqualStrings("orders", names[0]);
    try testing.expectEqual(@as(usize, 0), s.scanStreamNames(.default, &names));

    // Deleting one namespace's stream leaves the other's records and name.
    try testing.expect(s.deleteStream(a, nameKey(&na, one, "orders").?));
    try testing.expectEqual(@as(u64, 1), s.streamLogicalCount(b));
    try testing.expect(s.stream_names.contains(nameKey(&nb, two, "orders").?));
}

test "stream: stats tracking" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash = testKey(42);
    _ = try s.appendToStreamAt(hash, 100, 0, TEST_MS, 1, 0);
    _ = try s.appendToStreamAt(hash, 101, 0, TEST_MS, 1, 0);
    try testing.expectEqual(@as(u64, 2), s.stats.appended);
}

test "stream: memory usage estimate" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    _ = try s.appendToStreamAt(testKey(42), 100, 0, TEST_MS, 1, 0);

    try testing.expect(s.memoryUsage() > 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// StreamState Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "stream: StreamState append and read" {
    var ss = StreamState.init(testKey(42));
    defer ss.deinit(testing.allocator);

    const id1 = try ss.appendAt(testing.allocator, 100, 0, TEST_MS, 1, 0);
    const id2 = try ss.appendAt(testing.allocator, 101, 0, TEST_MS, 1, 0);
    const id3 = try ss.appendAt(testing.allocator, 102, 1, TEST_MS, 1, 0);

    try testing.expectEqual(@as(usize, 3), ss.count());
    try testing.expect(ss.first_id.eql(id1));
    try testing.expect(ss.last_id.eql(id3));

    // readRange: all records
    var buf: [10]StreamRecord = undefined;
    const n = ss.readRange(StreamID.MIN, StreamID.MAX, null, &buf);
    try testing.expectEqual(@as(usize, 3), n);

    // readRange: filtered by partition
    const n2 = ss.readRange(StreamID.MIN, StreamID.MAX, 1, &buf);
    try testing.expectEqual(@as(usize, 1), n2);
    try testing.expectEqual(@as(u64, 102), buf[0].ual_index);

    // readAfter
    const n3 = ss.readAfter(id1, null, &buf);
    try testing.expectEqual(@as(usize, 2), n3);
    try testing.expect(buf[0].id.eql(id2));
}

test "stream: StreamState readByIds preserves order and skips missing" {
    var ss = StreamState.init(testKey(42));
    defer ss.deinit(testing.allocator);

    const id1 = try ss.appendAt(testing.allocator, 100, 0, TEST_MS, 1, 0);
    const id2 = try ss.appendAt(testing.allocator, 101, 0, TEST_MS, 1, 0);
    const id3 = try ss.appendAt(testing.allocator, 102, 0, TEST_MS, 1, 0);
    const id4 = try ss.appendAt(testing.allocator, 103, 0, TEST_MS, 1, 0);

    // Sparse query: id3, id1 (out-of-order), id4 — should preserve input order
    const query = [_]StreamID{ id3, id1, id4 };
    var buf: [10]StreamRecord = undefined;
    const n = ss.readByIds(&query, &buf);
    try testing.expectEqual(@as(usize, 3), n);
    try testing.expect(buf[0].id.eql(id3));
    try testing.expect(buf[1].id.eql(id1));
    try testing.expect(buf[2].id.eql(id4));

    // Missing IDs are silently skipped
    const fake = StreamID{ .timestamp_ms = 999_999_999, .sequence = 999 };
    const query2 = [_]StreamID{ id2, fake, id3 };
    const n2 = ss.readByIds(&query2, &buf);
    try testing.expectEqual(@as(usize, 2), n2);
    try testing.expect(buf[0].id.eql(id2));
    try testing.expect(buf[1].id.eql(id3));

    // Empty input
    const empty: []const StreamID = &.{};
    try testing.expectEqual(@as(usize, 0), ss.readByIds(empty, &buf));

    // Buffer cap honored
    const all = [_]StreamID{ id1, id2, id3, id4 };
    var small: [2]StreamRecord = undefined;
    try testing.expectEqual(@as(usize, 2), ss.readByIds(&all, &small));
}

test "stream: StreamState trim" {
    var ss = StreamState.init(testKey(42));
    defer ss.deinit(testing.allocator);

    const id1 = try ss.appendAt(testing.allocator, 100, 0, TEST_MS, 1, 0);
    const id2 = try ss.appendAt(testing.allocator, 101, 0, TEST_MS, 1, 0);
    _ = try ss.appendAt(testing.allocator, 102, 0, TEST_MS, 1, 0);

    const trimmed = ss.trim(testing.allocator, id2);
    try testing.expectEqual(@as(u64, 2), trimmed);
    try testing.expectEqual(@as(usize, 1), ss.count());
    try testing.expect(!ss.first_id.eql(id1));
    try testing.expect(!ss.first_id.eql(id2));
}

test "stream: appendToStreamAt creates stream and returns StreamID" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash = testKey(12345);
    const id1 = try s.appendToStreamAt(hash, 100, 0, TEST_MS, 1, 0);
    const id2 = try s.appendToStreamAt(hash, 101, 0, TEST_MS, 1, 0);

    // StreamIDs are monotonically increasing
    try testing.expect(id2.greaterThan(id1));

    // Per-stream state recorded
    try testing.expectEqual(@as(usize, 2), s.streamRecordCount(hash));
    try testing.expect(s.streamFirstId(hash).eql(id1));
    try testing.expect(s.streamLastId(hash).eql(id2));
}

test "stream: the next append in the same millisecond is past a cursor at the previous one" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash = testKey(7);
    const first = try s.appendToStreamAt(hash, 1, 0, 1_000, 1, 0);
    const second = try s.appendToStreamAt(hash, 2, 0, 1_000, 1, 0);
    try testing.expectEqual(first.timestamp_ms, second.timestamp_ms);

    var buf: [4]StreamRecord = undefined;
    try testing.expectEqual(@as(usize, 1), s.readStreamAfter(hash, first, null, &buf));
    try testing.expect(buf[0].id.eql(second));
    try testing.expectEqual(@as(usize, 1), s.readStreamRange(hash, first, StreamID.MAX, null, &buf));
    try testing.expect(buf[0].id.eql(second));
}

test "stream: readStreamRange and readStreamAfter" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash = testKey(42);
    const id1 = try s.appendToStreamAt(hash, 100, 0, TEST_MS, 1, 0);
    _ = try s.appendToStreamAt(hash, 101, 0, TEST_MS, 1, 0);
    const id3 = try s.appendToStreamAt(hash, 102, 1, TEST_MS, 1, 0);

    // readStreamRange: all
    var buf: [10]StreamRecord = undefined;
    const n = s.readStreamRange(hash, StreamID.MIN, StreamID.MAX, null, &buf);
    try testing.expectEqual(@as(usize, 3), n);

    // readStreamRange: filtered by partition 1
    const n2 = s.readStreamRange(hash, StreamID.MIN, StreamID.MAX, 1, &buf);
    try testing.expectEqual(@as(usize, 1), n2);
    try testing.expect(buf[0].id.eql(id3));

    // readStreamAfter
    const n3 = s.readStreamAfter(hash, id1, null, &buf);
    try testing.expectEqual(@as(usize, 2), n3);

    // Non-existent stream returns 0
    const n4 = s.readStreamRange(testKey(999), StreamID.MIN, StreamID.MAX, null, &buf);
    try testing.expectEqual(@as(usize, 0), n4);
}

test "stream: trimStream" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash = testKey(42);
    _ = try s.appendToStreamAt(hash, 100, 0, TEST_MS, 1, 0);
    const id2 = try s.appendToStreamAt(hash, 101, 0, TEST_MS, 1, 0);
    _ = try s.appendToStreamAt(hash, 102, 0, TEST_MS, 1, 0);

    const trimmed = s.trimStream(hash, id2);
    try testing.expectEqual(@as(u64, 2), trimmed);
    try testing.expectEqual(@as(usize, 1), s.streamRecordCount(hash));
}

test "stream: multiple streams are independent" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash_a = testKey(1);
    const hash_b = testKey(2);

    const a1 = try s.appendToStreamAt(hash_a, 100, 0, TEST_MS, 1, 0);
    const a2 = try s.appendToStreamAt(hash_a, 101, 0, TEST_MS, 1, 0);
    _ = try s.appendToStreamAt(hash_b, 200, 0, TEST_MS, 1, 0);

    try testing.expectEqual(@as(usize, 2), s.streamRecordCount(hash_a));
    try testing.expectEqual(@as(usize, 1), s.streamRecordCount(hash_b));

    // Reading stream A returns only A's records
    var buf: [10]StreamRecord = undefined;
    const n = s.readStreamAfter(hash_a, a1, null, &buf);
    try testing.expectEqual(@as(usize, 1), n);
    try testing.expect(buf[0].id.eql(a2));
}

test "stream: batch append reserves one sequence per record" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash = testKey(42);
    const ts: u64 = 1_700_000_000_000;

    // A 3-record batch, then a single record in the SAME millisecond.
    const first = try s.appendToStreamAt(hash, 10, 0, ts, 3, 25);
    const next = try s.appendToStreamAt(hash, 11, 0, ts, 1, 9);

    // Reads expand the batch across sequences 0,1,2, so the following append
    // must start at 3. Reserving one sequence per batch would hand out
    // sequence 1 twice — duplicate StreamIDs within a single millisecond.
    try testing.expectEqual(@as(u64, 0), first.sequence);
    try testing.expectEqual(@as(u64, 3), next.sequence);
    try testing.expect(next.greaterThan(.{ .timestamp_ms = ts, .sequence = first.sequence + 2 }));
}

test "stream: info counts logical records and bytes, not entries" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash = testKey(43);
    const ts: u64 = 1_700_000_000_000;

    _ = try s.appendToStreamAt(hash, 10, 0, ts, 3, 25);
    _ = try s.appendToStreamAt(hash, 11, 0, ts + 1, 2, 14);

    // Two append entries carrying five records between them.
    try testing.expectEqual(@as(usize, 2), s.streamRecordCount(hash));
    try testing.expectEqual(@as(u64, 5), s.streamLogicalCount(hash));
    try testing.expectEqual(@as(u64, 39), s.streamByteSize(hash));

    // The last ID is the batch's last record, not its first.
    try testing.expectEqual(@as(u64, 1), s.streamLastId(hash).sequence);
    try testing.expectEqual(@as(u64, ts + 1), s.streamLastId(hash).timestamp_ms);
}

test "stream: trim decrements logical counters" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash = testKey(44);
    const ts: u64 = 1_700_000_000_000;

    const b1 = try s.appendToStreamAt(hash, 10, 0, ts, 3, 25);
    _ = try s.appendToStreamAt(hash, 11, 0, ts + 1, 2, 14);

    // Trim through the first batch's last record.
    _ = s.trimStream(hash, .{ .timestamp_ms = ts, .sequence = b1.sequence + 2 });

    try testing.expectEqual(@as(u64, 2), s.streamLogicalCount(hash));
    try testing.expectEqual(@as(u64, 14), s.streamByteSize(hash));

    // Trimming everything zeroes both counters.
    _ = s.trimStream(hash, .{ .timestamp_ms = ts + 1, .sequence = 1 });
    try testing.expectEqual(@as(u64, 0), s.streamLogicalCount(hash));
    try testing.expectEqual(@as(u64, 0), s.streamByteSize(hash));
}

test "stream: count retention resolves by records, not batches" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash = testKey(45);
    const ts: u64 = 1_700_000_000_000;

    _ = try s.appendToStreamAt(hash, 10, 0, ts, 3, 25);
    _ = try s.appendToStreamAt(hash, 11, 0, ts + 1, 3, 25);
    _ = try s.appendToStreamAt(hash, 12, 0, ts + 2, 3, 25);

    // Nine records across three batches. Dropping "the oldest four" can only
    // cut on an entry boundary, so it cuts after the first batch (3 records) —
    // never past it, which would delete records still inside the window.
    const cut = s.resolveNthRecordId(hash, 4);
    try testing.expectEqual(@as(u64, ts), cut.timestamp_ms);
    try testing.expectEqual(@as(u64, 2), cut.sequence);

    _ = s.trimStream(hash, cut);
    try testing.expectEqual(@as(u64, 6), s.streamLogicalCount(hash));

    // Indexing entries instead of records would have cut at entry 4 — past the
    // end — and dropped all nine.
    try testing.expect(s.streamLogicalCount(hash) > 0);

    // A budget smaller than the first batch trims nothing at all.
    try testing.expect(s.resolveNthRecordId(hash, 1).eql(StreamID.MIN));
}

test "stream: trimCount predicts trim in records, not batches" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash = testKey(45);
    const ts: u64 = 1_700_000_000_000;

    const b1 = try s.appendToStreamAt(hash, 10, 0, ts, 3, 25);
    const b2 = try s.appendToStreamAt(hash, 11, 0, ts + 1, 2, 14);
    _ = try s.appendToStreamAt(hash, 12, 0, ts + 2, 4, 30);

    const through: StreamID = .{ .timestamp_ms = ts + 1, .sequence = b2.sequence + 1 };
    try testing.expectEqual(@as(u64, 5), s.trimCount(hash, through));
    const left = s.firstIdAfterTrim(hash, through);
    try testing.expectEqual(@as(u64, 9), s.streamLogicalCount(hash));

    try testing.expectEqual(@as(u64, 5), s.trimStream(hash, through));
    try testing.expect(s.streamFirstId(hash).eql(left));
    try testing.expectEqual(@as(u64, 4), s.streamLogicalCount(hash));

    // A boundary before every record removes nothing.
    try testing.expectEqual(@as(u64, 0), s.trimCount(hash, .{ .timestamp_ms = b1.timestamp_ms - 1, .sequence = 0 }));
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
