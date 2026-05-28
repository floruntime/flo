//! Stream Projection — per-stream record tracking + consumer groups with PEL.
//!
//! Each stream has its own StreamID space (timestamp-sequence, like Redis).
//! Consumer groups use a Pending Entry List (PEL) for per-message ack/nack/claim.
//!
//! Data flow:
//!   stream_append → per-stream ID gen → record StreamID → UAL index mapping
//!   group_read    → deliver from last_delivered_id → add to PEL
//!   group_ack     → remove from PEL by StreamID
//!   group_nack    → mark for redelivery in PEL
//!   group_claim   → transfer PEL entry to new consumer
//!   stream_trim   → remove records with IDs below threshold
//!
//! Consumer group lifecycle:
//!   create → join member → read (adds to PEL) → ack/nack → claim → leave → delete

const std = @import("std");
const Allocator = std.mem.Allocator;
const entry_mod = @import("../storage/ual/entry.zig");
const router_mod = @import("router.zig");
const stream_id_mod = @import("../stream/stream_id.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const CommandPayload = entry_mod.CommandPayload;
pub const StreamID = stream_id_mod.StreamID;
pub const StreamIdGenerator = stream_id_mod.StreamIdGenerator;

/// Default consumer-group ack timeout: a delivered-but-unacked entry idle this
/// long is re-nacked by the background sweeper (made claimable again).
pub const DEFAULT_ACK_TIMEOUT_MS: u32 = 30_000;
/// Default max delivery attempts before the sweeper drops a poison entry.
pub const DEFAULT_MAX_DELIVER: u8 = 10;

// ═══════════════════════════════════════════════════════════════════════════════
// Consumer Group Member
// ═══════════════════════════════════════════════════════════════════════════════

pub const MemberState = enum(u8) {
    active,
    leaving,
};

pub const Member = struct {
    /// Member identifier (e.g., consumer instance ID).
    id: []const u8,
    /// Timestamp when joined.
    joined_at_ns: u64,
    /// Current state.
    state: MemberState,
    /// Number of PEL entries assigned to this consumer.
    pending_count: u32 = 0,
    /// Last time this consumer was active (ms since epoch).
    last_active_ms: u64 = 0,
};

// ═══════════════════════════════════════════════════════════════════════════════
// PEL (Pending Entry List) — tracks delivered-but-unacked messages
// ═══════════════════════════════════════════════════════════════════════════════

pub const PendingEntry = struct {
    /// The StreamID of this pending message.
    id: StreamID,
    /// Consumer currently owning this entry.
    consumer: []const u8,
    /// When first delivered (ms since epoch).
    delivered_at_ms: u64,
    /// Most recent delivery/claim time (ms since epoch).
    last_delivery_ms: u64,
    /// How many times delivered.
    delivery_count: u32,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Per-Stream Record — maps StreamID → UAL index
// ═══════════════════════════════════════════════════════════════════════════════

pub const StreamRecord = struct {
    /// The StreamID assigned to this record.
    id: StreamID,
    /// UAL index where the record's data lives.
    ual_index: u64,
    /// User partition index (for multi-partition streams).
    partition_index: u32,
};

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
    /// Trim threshold — records at or below this are considered trimmed.
    trim_id: StreamID,
    /// Stream name hash (for cross-referencing).
    name_hash: u64,

    pub fn init(name_hash: u64) StreamState {
        return .{
            .id_gen = StreamIdGenerator.init(),
            .records = .empty,
            .first_id = StreamID.MIN,
            .last_id = StreamID.MIN,
            .trim_id = StreamID.MIN,
            .name_hash = name_hash,
        };
    }

    pub fn deinit(self: *StreamState, allocator: Allocator) void {
        self.records.deinit(allocator);
    }

    /// Append a record. Generates a new StreamID and returns it.
    pub fn append(self: *StreamState, allocator: Allocator, ual_index: u64, partition_index: u32) !StreamID {
        const id = self.id_gen.next();
        try self.records.append(allocator, .{
            .id = id,
            .ual_index = ual_index,
            .partition_index = partition_index,
        });
        if (self.first_id.eql(StreamID.MIN)) {
            self.first_id = id;
        }
        self.last_id = id;
        return id;
    }

    /// Read records with IDs in [from_id, to_id] inclusive.
    pub fn readRange(self: *const StreamState, from_id: StreamID, to_id: StreamID, filter_partition: ?u32, buf: []StreamRecord) usize {
        const items = self.records.items;
        if (items.len == 0) return 0;
        const start_idx = self.lowerBound(from_id);
        if (start_idx >= items.len) return 0;
        var n: usize = 0;
        var i = start_idx;
        while (i < items.len and n < buf.len) : (i += 1) {
            const rec = &items[i];
            if (rec.id.greaterThan(to_id)) break;
            if (rec.id.lessThan(from_id)) continue;
            if (filter_partition) |fp| {
                if (rec.partition_index != fp) continue;
            }
            buf[n] = rec.*;
            n += 1;
        }
        return n;
    }

    /// Read records after a given ID (exclusive).
    pub fn readAfter(self: *const StreamState, after_id: StreamID, filter_partition: ?u32, buf: []StreamRecord) usize {
        const items = self.records.items;
        if (items.len == 0) return 0;
        // Find first record strictly after after_id
        var start_idx = self.lowerBound(after_id);
        // Skip records equal to after_id
        while (start_idx < items.len and items[start_idx].id.eql(after_id)) : (start_idx += 1) {}
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
    ///
    /// Used by `stream_group_claim`: the projection's `ConsumerGroup.autoclaim`
    /// returns just the IDs it claimed, and the wire handler resolves them to
    /// full records (payload + headers) in one round-trip before sending the
    /// response. Avoids a sparse `readRange` that would pull in unrelated IDs.
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

    /// Number of live records.
    pub fn count(self: *const StreamState) usize {
        return self.records.items.len;
    }

    /// Trim records with IDs <= up_to_id. Returns count trimmed.
    pub fn trim(self: *StreamState, allocator: Allocator, up_to_id: StreamID) u64 {
        _ = allocator;
        const items = self.records.items;
        if (items.len == 0) return 0;

        // Find the first record that is strictly greater than up_to_id
        var cut: usize = 0;
        while (cut < items.len) : (cut += 1) {
            if (items[cut].id.greaterThan(up_to_id)) break;
        }

        if (cut == 0) {
            self.trim_id = up_to_id;
            return 0;
        }

        // Shift remaining records to front
        const remaining = items.len - cut;
        if (remaining > 0) {
            std.mem.copyForwards(StreamRecord, items[0..remaining], items[cut..items.len]);
        }
        self.records.items.len = remaining;

        self.trim_id = up_to_id;
        if (remaining > 0) {
            self.first_id = self.records.items[0].id;
        } else {
            self.first_id = StreamID.MIN;
            self.last_id = StreamID.MIN;
        }
        return @intCast(cut);
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

// ═══════════════════════════════════════════════════════════════════════════════
// Consumer Group
// ═══════════════════════════════════════════════════════════════════════════════

pub const ConsumerGroup = struct {
    /// Group name (owned).
    name: []const u8,
    /// Members by ID.
    members: std.StringHashMap(Member),
    /// When the group was created.
    created_at_ns: u64,
    /// Allocator for member management.
    allocator: Allocator,
    /// PEL — StreamID.order() → PendingEntry. Tracks delivered-but-unacked
    /// messages. Array-backed so iteration order is stable for cursor-based
    /// scans (`autoclaim` paginates with a `start_id` cursor). Inserts happen
    /// at delivery time in monotonic StreamID order (`groupDeliver` reads
    /// past `last_delivered_id`); removals use `fetchOrderedRemove` so the
    /// order invariant is preserved.
    pel: std.AutoArrayHashMapUnmanaged(u128, PendingEntry),
    /// Highest StreamID delivered to any consumer in this group.
    last_delivered_id: StreamID,
    /// Idle time (ms) after which the background sweeper re-nacks a pending
    /// entry (makes it claimable again). 0 = sweeper disabled for this group.
    /// In-memory config — groups are not persisted (auto-created on read), so
    /// this resets to the default on restart; see handleGroupCreate /
    /// stream_group_configure_sweeper for runtime overrides. (FLO-102)
    ack_timeout_ms: u32 = DEFAULT_ACK_TIMEOUT_MS,
    /// Max delivery attempts before the sweeper drops an entry from the PEL
    /// (poison-message guard). 0 = unlimited.
    max_deliver: u8 = DEFAULT_MAX_DELIVER,

    pub fn init(allocator: Allocator, name: []const u8, created_at_ns: u64) !ConsumerGroup {
        const owned_name = try allocator.dupe(u8, name);
        return .{
            .name = owned_name,
            .members = std.StringHashMap(Member).init(allocator),
            .created_at_ns = created_at_ns,
            .allocator = allocator,
            .pel = .empty,
            .last_delivered_id = StreamID.MIN,
        };
    }

    pub fn deinit(self: *ConsumerGroup) void {
        var it = self.members.iterator();
        while (it.next()) |kv| {
            self.allocator.free(@constCast(kv.value_ptr.id));
        }
        self.members.deinit();
        self.pel.deinit(self.allocator);
        self.allocator.free(@constCast(self.name));
    }

    /// Add a member to the group. Returns true if new, false if already exists.
    pub fn join(self: *ConsumerGroup, member_id: []const u8, now_ns: u64) !bool {
        const gop = try self.members.getOrPut(member_id);
        if (gop.found_existing) return false;
        const owned_id = try self.allocator.dupe(u8, member_id);
        gop.key_ptr.* = owned_id;
        gop.value_ptr.* = .{
            .id = owned_id,
            .joined_at_ns = now_ns,
            .state = .active,
        };
        return true;
    }

    /// Remove a member from the group. Returns true if removed.
    pub fn leave(self: *ConsumerGroup, member_id: []const u8) bool {
        if (self.members.fetchRemove(member_id)) |kv| {
            self.allocator.free(@constCast(kv.value.id));
            return true;
        }
        return false;
    }

    pub fn memberCount(self: *const ConsumerGroup) usize {
        return self.members.count();
    }

    // ── PEL Operations ──────────────────────────────────────────────────

    /// Deliver records to a consumer — adds them to the PEL.
    /// `records` are the StreamRecords being delivered. Returns count added to PEL.
    pub fn deliver(self: *ConsumerGroup, consumer_id: []const u8, records: []const StreamRecord, now_ms: u64) !u32 {
        var added: u32 = 0;
        for (records) |rec| {
            const key = rec.id.order();
            const gop = try self.pel.getOrPut(self.allocator, key);
            if (!gop.found_existing) {
                gop.value_ptr.* = .{
                    .id = rec.id,
                    .consumer = consumer_id,
                    .delivered_at_ms = now_ms,
                    .last_delivery_ms = now_ms,
                    .delivery_count = 1,
                };
                added += 1;
                if (self.members.getPtr(consumer_id)) |m| {
                    m.pending_count += 1;
                    m.last_active_ms = now_ms;
                }
            }
        }
        // Advance last_delivered_id
        if (records.len > 0) {
            const last = records[records.len - 1].id;
            if (last.greaterThan(self.last_delivered_id)) {
                self.last_delivered_id = last;
            }
        }
        return added;
    }

    /// Acknowledge messages — remove from PEL. Returns count acked.
    ///
    /// Uses `fetchOrderedRemove` (not `fetchSwapRemove`) so the PEL stays
    /// in insertion order, which equals StreamID order since deliveries
    /// happen monotonically. `autoclaim`'s cursor-based scan depends on it.
    pub fn ack(self: *ConsumerGroup, ids: []const StreamID) u32 {
        var acked: u32 = 0;
        for (ids) |id| {
            const key = id.order();
            if (self.pel.fetchOrderedRemove(key)) |kv| {
                if (self.members.getPtr(kv.value.consumer)) |m| {
                    if (m.pending_count > 0) m.pending_count -= 1;
                }
                acked += 1;
            }
        }
        return acked;
    }

    /// Negative-acknowledge messages — mark for redelivery (increment delivery_count, reset timer).
    pub fn nack(self: *ConsumerGroup, ids: []const StreamID, now_ms: u64) u32 {
        var nacked: u32 = 0;
        for (ids) |id| {
            const key = id.order();
            if (self.pel.getPtr(key)) |pe| {
                pe.delivery_count += 1;
                pe.last_delivery_ms = now_ms;
                nacked += 1;
            }
        }
        return nacked;
    }

    /// Claim specific PEL entries for a new consumer. Returns count claimed.
    pub fn claim(self: *ConsumerGroup, ids: []const StreamID, new_consumer: []const u8, min_idle_ms: u64, now_ms: u64) u32 {
        var claimed: u32 = 0;
        for (ids) |id| {
            const key = id.order();
            if (self.pel.getPtr(key)) |pe| {
                const idle = if (now_ms > pe.last_delivery_ms) now_ms - pe.last_delivery_ms else 0;
                if (idle >= min_idle_ms) {
                    // Decrement old consumer count
                    if (self.members.getPtr(pe.consumer)) |m| {
                        if (m.pending_count > 0) m.pending_count -= 1;
                    }
                    pe.consumer = new_consumer;
                    pe.last_delivery_ms = now_ms;
                    pe.delivery_count += 1;
                    // Increment new consumer count
                    if (self.members.getPtr(new_consumer)) |m| {
                        m.pending_count += 1;
                    }
                    claimed += 1;
                }
            }
        }
        return claimed;
    }

    /// Result of an `autoclaim` cursor scan.
    pub const ClaimResult = struct {
        /// Number of entries claimed (written to `out`).
        count: u32,
        /// Cursor to pass as `start_id` on the next call. `StreamID.MAX` means
        /// the PEL has been fully scanned (no more entries past this batch).
        next_cursor: StreamID,
    };

    /// Auto-claim PEL entries for a consumer, scanning in StreamID order from
    /// `start_id`. Writes claimed IDs to `out`; returns the count plus a cursor
    /// for the next page.
    ///
    /// Single cursor-based scan covers two use cases:
    ///   * **Drain own pending** (reconnect): `new_consumer = me`, `min_idle_ms = 0`.
    ///     Re-delivers entries this consumer already owns (bumps `delivery_count`,
    ///     touches `last_delivery_ms`). Same-owner is **not** skipped — that's
    ///     the whole point of the reconnect drain.
    ///   * **Steal from idle consumers** (rebalance): `min_idle_ms > 0`.
    ///     Transfers ownership to `new_consumer` for entries idle ≥ `min_idle_ms`.
    ///     Entries that don't meet the idle threshold are passed over and the
    ///     cursor advances past them (a scan covers each entry once).
    ///
    /// Relies on the array-backed PEL iterating in insertion order (== StreamID
    /// order, since deliveries are monotonic and acks use `fetchOrderedRemove`).
    pub fn autoclaim(self: *ConsumerGroup, new_consumer: []const u8, min_idle_ms: u64, now_ms: u64, start_id: StreamID, max_count: usize, out: []StreamID) ClaimResult {
        var result_count: u32 = 0;
        const start_order = start_id.order();
        var next_cursor = StreamID.MAX;
        var it = self.pel.iterator();
        while (it.next()) |kv| {
            if (kv.key_ptr.* < start_order) continue;
            // Stop once the batch is full; the next unprocessed key becomes the
            // cursor so the caller resumes exactly where we left off.
            if (result_count >= max_count or result_count >= out.len) {
                next_cursor = StreamID.fromOrder(kv.key_ptr.*);
                break;
            }
            const pe = kv.value_ptr;
            const idle = if (now_ms > pe.last_delivery_ms) now_ms - pe.last_delivery_ms else 0;
            if (idle >= min_idle_ms) {
                const same_owner = std.mem.eql(u8, pe.consumer, new_consumer);
                if (!same_owner) {
                    if (self.members.getPtr(pe.consumer)) |m| {
                        if (m.pending_count > 0) m.pending_count -= 1;
                    }
                    pe.consumer = new_consumer;
                    if (self.members.getPtr(new_consumer)) |m| {
                        m.pending_count += 1;
                    }
                }
                pe.last_delivery_ms = now_ms;
                pe.delivery_count += 1;
                out[result_count] = pe.id;
                result_count += 1;
            }
        }
        return .{ .count = result_count, .next_cursor = next_cursor };
    }

    /// Touch pending entries — reset their idle timer.
    pub fn touch(self: *ConsumerGroup, ids: []const StreamID, now_ms: u64) u32 {
        var touched: u32 = 0;
        for (ids) |id| {
            const key = id.order();
            if (self.pel.getPtr(key)) |pe| {
                pe.last_delivery_ms = now_ms;
                touched += 1;
            }
        }
        return touched;
    }

    /// Get pending entries, optionally for a specific consumer.
    pub fn getPending(self: *const ConsumerGroup, consumer_id: ?[]const u8, buf: []PendingEntry) usize {
        var n: usize = 0;
        var it = self.pel.iterator();
        while (it.next()) |kv| {
            if (n >= buf.len) break;
            const pe = kv.value_ptr;
            if (consumer_id) |cid| {
                if (!std.mem.eql(u8, pe.consumer, cid)) continue;
            }
            buf[n] = pe.*;
            n += 1;
        }
        return n;
    }

    /// Number of entries in the PEL.
    pub fn pelCount(self: *const ConsumerGroup) usize {
        return self.pel.count();
    }

    /// Update sweeper config. A value of `null` leaves that field unchanged.
    pub fn configure(self: *ConsumerGroup, ack_timeout_ms: ?u32, max_deliver: ?u8) void {
        if (ack_timeout_ms) |v| self.ack_timeout_ms = v;
        if (max_deliver) |v| self.max_deliver = v;
    }

    /// Result of a sweeper pass over one group's PEL.
    pub const SweepResult = struct {
        /// Entries re-nacked (idle ≥ ack_timeout_ms): made claimable, delivery_count bumped.
        renacked: u32 = 0,
        /// Entries dropped from the PEL for exceeding max_deliver (poison).
        dropped: u32 = 0,
    };

    /// Background-sweeper pass: for each PEL entry idle ≥ `ack_timeout_ms`,
    /// either drop it (delivery_count > max_deliver, poison guard) or re-nack
    /// it — reset the idle timer and bump delivery_count so a subsequent
    /// claim/redelivery picks it up. No-op when ack_timeout_ms == 0.
    ///
    /// Dropped entries are removed with `fetchOrderedRemove` to preserve PEL
    /// ordering (autoclaim's cursor invariant). Returns counts.
    pub fn sweep(self: *ConsumerGroup, now_ms: u64) SweepResult {
        if (self.ack_timeout_ms == 0) return .{};
        var res: SweepResult = .{};

        // Collect drop targets first; mutating the array map during iteration
        // would invalidate the iterator.
        var drop_buf: [256]u128 = undefined;
        var drop_n: usize = 0;

        var it = self.pel.iterator();
        while (it.next()) |kv| {
            const pe = kv.value_ptr;
            const idle = if (now_ms > pe.last_delivery_ms) now_ms - pe.last_delivery_ms else 0;
            if (idle < self.ack_timeout_ms) continue;

            if (self.max_deliver > 0 and pe.delivery_count >= self.max_deliver) {
                if (drop_n < drop_buf.len) {
                    drop_buf[drop_n] = kv.key_ptr.*;
                    drop_n += 1;
                }
            } else {
                // Re-nack: make the entry eligible for redelivery/claim.
                pe.delivery_count += 1;
                pe.last_delivery_ms = now_ms;
                res.renacked += 1;
            }
        }

        for (drop_buf[0..drop_n]) |key| {
            if (self.pel.fetchOrderedRemove(key)) |kv| {
                if (self.members.getPtr(kv.value.consumer)) |m| {
                    if (m.pending_count > 0) m.pending_count -= 1;
                }
                res.dropped += 1;
            }
        }
        return res;
    }
};

/// Stream metadata — partition count, retention policy, and other per-stream config.
pub const StreamMetadata = struct {
    partition_count: u32 = 1,
    /// Pre-computed name hash for background trim lookups.
    name_hash: u64 = 0,
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

    /// Consumer groups by name.
    groups: std.StringHashMap(ConsumerGroup),

    /// Registered stream names (for listing).
    stream_names: std.StringHashMap(void),

    /// Stream metadata (partition count, etc.).
    stream_metadata: std.StringHashMap(StreamMetadata),

    /// Per-stream state — each stream has its own StreamID space.
    streams: std.AutoHashMap(u64, StreamState),

    /// Last applied UAL index.
    applied_index: u64,

    /// Stats.
    stats: Stats,

    pub const Stats = struct {
        appended: u64 = 0,
        reads: u64 = 0,
        commits: u64 = 0,
        groups_created: u64 = 0,
        groups_deleted: u64 = 0,
        trimmed: u64 = 0,
    };

    pub fn init(allocator: Allocator) StreamProjection {
        return .{
            .allocator = allocator,
            .groups = std.StringHashMap(ConsumerGroup).init(allocator),
            .stream_names = std.StringHashMap(void).init(allocator),
            .stream_metadata = std.StringHashMap(StreamMetadata).init(allocator),
            .streams = std.AutoHashMap(u64, StreamState).init(allocator),
            .applied_index = 0,
            .stats = .{},
        };
    }

    pub fn deinit(self: *StreamProjection) void {
        // Free consumer groups
        var git = self.groups.iterator();
        while (git.next()) |kv| {
            kv.value_ptr.deinit();
        }
        self.groups.deinit();
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
    /// Used during namespace force-delete to clear all stream data.
    pub fn reset(self: *StreamProjection) void {
        // Free consumer groups
        var git = self.groups.iterator();
        while (git.next()) |kv| {
            kv.value_ptr.deinit();
        }
        self.groups.clearAndFree();
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
        self.applied_index = 0;
        self.stats = .{};
    }

    // ─── Consumer Group operations ─────────────────────────────────────────

    /// Create a consumer group. Returns error.AlreadyExists if name taken.
    pub fn createGroup(self: *StreamProjection, name: []const u8, now_ns: u64) !void {
        const gop = try self.groups.getOrPut(name);
        if (gop.found_existing) return error.AlreadyExists;

        var group = try ConsumerGroup.init(self.allocator, name, now_ns);
        gop.key_ptr.* = group.name;
        gop.value_ptr.* = group;
        _ = &group;
        self.stats.groups_created += 1;
    }

    /// Delete a consumer group.
    pub fn deleteGroup(self: *StreamProjection, name: []const u8) bool {
        if (self.groups.fetchRemove(name)) |kv| {
            var group = kv.value;
            group.deinit();
            self.stats.groups_deleted += 1;
            return true;
        }
        return false;
    }

    /// Get a consumer group by name.
    pub fn getGroup(self: *StreamProjection, name: []const u8) ?*ConsumerGroup {
        return self.groups.getPtr(name);
    }

    /// Join a member to a consumer group.
    pub fn joinGroup(self: *StreamProjection, group_name: []const u8, member_id: []const u8, now_ns: u64) !bool {
        if (self.groups.getPtr(group_name)) |group| {
            return try group.join(member_id, now_ns);
        }
        return error.GroupNotFound;
    }

    /// Remove a member from a consumer group.
    pub fn leaveGroup(self: *StreamProjection, group_name: []const u8, member_id: []const u8) !bool {
        if (self.groups.getPtr(group_name)) |group| {
            return group.leave(member_id);
        }
        return error.GroupNotFound;
    }

    /// Count of consumer groups.   
    pub fn groupCount(self: *const StreamProjection) usize {
        return self.groups.count();
    }

    // ─── PEL-aware Stream Operations ───────────────────────────────────────

    /// Get or create per-stream state for a given name hash.
    pub fn getOrCreateStream(self: *StreamProjection, name_hash: u64) !*StreamState {
        const gop = try self.streams.getOrPut(name_hash);
        if (!gop.found_existing) {
            gop.value_ptr.* = StreamState.init(name_hash);
        }
        return gop.value_ptr;
    }

    /// Append a record to a named stream. Returns the assigned StreamID.
    pub fn appendToStream(self: *StreamProjection, name_hash: u64, ual_index: u64, partition_index: u32) !StreamID {
        const ss = try self.getOrCreateStream(name_hash);
        const id = try ss.append(self.allocator, ual_index, partition_index);
        self.stats.appended += 1;
        return id;
    }

    /// Read records from a stream by ID range [from_id, to_id] inclusive.
    pub fn readStreamRange(self: *StreamProjection, name_hash: u64, from_id: StreamID, to_id: StreamID, filter_partition: ?u32, buf: []StreamRecord) usize {
        const ss = self.streams.getPtr(name_hash) orelse return 0;
        const n = ss.readRange(from_id, to_id, filter_partition, buf);
        self.stats.reads += n;
        return n;
    }

    /// Read records from a stream after a given ID (exclusive).
    pub fn readStreamAfter(self: *StreamProjection, name_hash: u64, after_id: StreamID, filter_partition: ?u32, buf: []StreamRecord) usize {
        const ss = self.streams.getPtr(name_hash) orelse return 0;
        const n = ss.readAfter(after_id, filter_partition, buf);
        self.stats.reads += n;
        return n;
    }

    /// Resolve specific StreamIDs back to records (preserves input order, skips
    /// missing IDs). Used by `stream_group_claim` to turn claimed PEL IDs into
    /// full records (payload + headers) for the wire response.
    pub fn readStreamByIds(self: *StreamProjection, name_hash: u64, ids: []const StreamID, buf: []StreamRecord) usize {
        const ss = self.streams.getPtr(name_hash) orelse return 0;
        const n = ss.readByIds(ids, buf);
        self.stats.reads += n;
        return n;
    }

    /// Get the last StreamID for a stream.
    pub fn streamLastId(self: *const StreamProjection, name_hash: u64) StreamID {
        const ss = self.streams.getPtr(name_hash) orelse return StreamID.MIN;
        return ss.last_id;
    }

    /// Get the first StreamID for a stream.
    pub fn streamFirstId(self: *const StreamProjection, name_hash: u64) StreamID {
        const ss = self.streams.getPtr(name_hash) orelse return StreamID.MIN;
        return ss.first_id;
    }

    /// Get the record count for a stream.
    pub fn streamRecordCount(self: *const StreamProjection, name_hash: u64) usize {
        const ss = self.streams.getPtr(name_hash) orelse return 0;
        return ss.count();
    }

    /// Trim a stream — remove records with IDs <= up_to_id.
    pub fn trimStream(self: *StreamProjection, name_hash: u64, up_to_id: StreamID) u64 {
        const ss = self.streams.getPtr(name_hash) orelse return 0;
        const trimmed = ss.trim(self.allocator, up_to_id);
        self.stats.trimmed += trimmed;
        return trimmed;
    }

    /// Trim the first `count` records from a named stream.
    /// Returns the number of records actually removed.
    pub fn trimStreamByCount(self: *StreamProjection, name_hash: u64, count: u64) u64 {
        const ss = self.streams.getPtr(name_hash) orelse return 0;
        const records = ss.records.items;
        if (records.len == 0 or count == 0) return 0;
        // Find the ID of the Nth record (or last if count >= len)
        const idx = @min(count, records.len) - 1;
        const up_to_id = records[idx].id;
        const trimmed = ss.trim(self.allocator, up_to_id);
        self.stats.trimmed += trimmed;
        return trimmed;
    }

    /// Resolve the StreamID of the Nth record in a stream (1-indexed).
    /// Used by trim-by-count to convert a count to a deterministic StreamID
    /// before persisting through Raft.
    pub fn resolveNthRecordId(self: *const StreamProjection, name_hash: u64, count: u64) StreamID {
        const ss = self.streams.getPtr(name_hash) orelse return StreamID.MIN;
        const records = ss.records.items;
        if (records.len == 0 or count == 0) return StreamID.MIN;
        const idx = @min(count, records.len) - 1;
        return records[idx].id;
    }

    /// Deliver records from a named stream to a consumer group.
    /// Reads `count` records after the group's last_delivered_id, adds to PEL.
    /// Returns the StreamRecords delivered (fills buf, returns count).
    pub fn groupDeliver(self: *StreamProjection, group_name: []const u8, name_hash: u64, consumer_id: []const u8, count: usize, now_ms: u64, buf: []StreamRecord) !usize {
        const group = self.groups.getPtr(group_name) orelse return error.GroupNotFound;
        const ss = self.streams.getPtr(name_hash) orelse return 0;

        const n = ss.readAfter(group.last_delivered_id, null, buf[0..@min(count, buf.len)]);
        if (n > 0) {
            _ = try group.deliver(consumer_id, buf[0..n], now_ms);
        }
        return n;
    }

    /// Acknowledge messages in a consumer group by StreamID.
    pub fn groupAck(self: *StreamProjection, group_name: []const u8, ids: []const StreamID) !u32 {
        const group = self.groups.getPtr(group_name) orelse return error.GroupNotFound;
        return group.ack(ids);
    }

    /// Negative-acknowledge messages in a consumer group (mark for redelivery).
    pub fn groupNack(self: *StreamProjection, group_name: []const u8, ids: []const StreamID, now_ms: u64) !u32 {
        const group = self.groups.getPtr(group_name) orelse return error.GroupNotFound;
        return group.nack(ids, now_ms);
    }

    /// Claim specific PEL entries for a new consumer.
    pub fn groupClaim(self: *StreamProjection, group_name: []const u8, ids: []const StreamID, consumer: []const u8, min_idle_ms: u64, now_ms: u64) !u32 {
        const group = self.groups.getPtr(group_name) orelse return error.GroupNotFound;
        return group.claim(ids, consumer, min_idle_ms, now_ms);
    }

    /// Auto-claim PEL entries in a consumer group (cursor-based scan).
    pub fn groupAutoclaim(self: *StreamProjection, group_name: []const u8, consumer: []const u8, min_idle_ms: u64, now_ms: u64, start_id: StreamID, max_count: usize, out: []StreamID) !ConsumerGroup.ClaimResult {
        const group = self.groups.getPtr(group_name) orelse return error.GroupNotFound;
        return group.autoclaim(consumer, min_idle_ms, now_ms, start_id, max_count, out);
    }

    /// Touch (reset idle timer) for PEL entries.
    pub fn groupTouch(self: *StreamProjection, group_name: []const u8, ids: []const StreamID, now_ms: u64) !u32 {
        const group = self.groups.getPtr(group_name) orelse return error.GroupNotFound;
        return group.touch(ids, now_ms);
    }

    /// Get pending entries for a consumer group (optionally filtered by consumer).
    pub fn groupPending(self: *StreamProjection, group_name: []const u8, consumer_id: ?[]const u8, buf: []PendingEntry) !usize {
        const group = self.groups.getPtr(group_name) orelse return error.GroupNotFound;
        return group.getPending(consumer_id, buf);
    }

    /// Get the PEL count for a consumer group.
    pub fn groupPelCount(self: *StreamProjection, group_name: []const u8) !usize {
        const group = self.groups.getPtr(group_name) orelse return error.GroupNotFound;
        return group.pelCount();
    }

    /// Update a group's sweeper config (ack_timeout_ms / max_deliver).
    /// `null` leaves a field unchanged.
    pub fn configureGroup(self: *StreamProjection, group_name: []const u8, ack_timeout_ms: ?u32, max_deliver: ?u8) !void {
        const group = self.groups.getPtr(group_name) orelse return error.GroupNotFound;
        group.configure(ack_timeout_ms, max_deliver);
    }

    /// Run the sweeper pass over every consumer group in this projection.
    /// Returns the aggregate counts. Called from the shard's periodic task.
    pub fn sweepAllGroups(self: *StreamProjection, now_ms: u64) ConsumerGroup.SweepResult {
        var total: ConsumerGroup.SweepResult = .{};
        var it = self.groups.iterator();
        while (it.next()) |kv| {
            const r = kv.value_ptr.sweep(now_ms);
            total.renacked += r.renacked;
            total.dropped += r.dropped;
        }
        return total;
    }

    // ─── Stream Name Registry ──────────────────────────────────────────────

    /// Register a stream name. No-op if already registered.
    pub fn registerStream(self: *StreamProjection, name: []const u8) !void {
        if (name.len == 0) return;
        const gop = try self.stream_names.getOrPut(name);
        if (!gop.found_existing) {
            const owned = try self.allocator.dupe(u8, name);
            gop.key_ptr.* = owned;
        }
    }

    /// Register (or update) metadata for a stream.
    pub fn registerStreamMetadata(self: *StreamProjection, name: []const u8, meta: StreamMetadata) !void {
        if (name.len == 0) return;
        const gop = try self.stream_metadata.getOrPut(name);
        if (!gop.found_existing) {
            const owned = try self.allocator.dupe(u8, name);
            gop.key_ptr.* = owned;
        }
        gop.value_ptr.* = .{
            .partition_count = @max(1, meta.partition_count),
            .name_hash = meta.name_hash,
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

    /// Scan registered stream names into a buffer. Returns the count written.
    /// Returns borrowed references into the HashMap (zero-copy, valid until mutation).
    pub fn scanStreamNames(self: *const StreamProjection, buf: [][]const u8) usize {
        var count: usize = 0;
        var it = self.stream_names.keyIterator();
        while (it.next()) |key| {
            if (count >= buf.len) break;
            buf[count] = key.*;
            count += 1;
        }
        return count;
    }

    /// Number of registered stream names.
    pub fn streamCount(self: *const StreamProjection) usize {
        return self.stream_names.count();
    }

    // ─── UAL Entry application ─────────────────────────────────────────────

    pub fn applyEntry(self: *StreamProjection, ual_entry: *const Entry) !void {
        const entry_type: EntryType = @enumFromInt(ual_entry.header.entry_type);

        switch (entry_type) {
            .stream_append => {
                const name_hash = if (CommandPayload.deserialize(ual_entry.payload)) |cmd|
                    std.hash.Wyhash.hash(@as(u64, cmd.namespace_hash), cmd.key)
                else
                    0;
                _ = try self.appendToStream(name_hash, ual_entry.header.index, 0);
            },
            .stream_trim => {
                // Trim target encoded as StreamID (timestamp_ms + sequence) in command payload key
                if (CommandPayload.deserialize(ual_entry.payload)) |cmd| {
                    if (cmd.key.len >= 16) {
                        const ts = std.mem.readInt(u64, cmd.key[0..8], .little);
                        const seq = std.mem.readInt(u64, cmd.key[8..16], .little);
                        const name_hash = if (cmd.value.len >= 8) std.mem.readInt(u64, cmd.value[0..8], .little) else 0;
                        _ = self.trimStream(name_hash, .{ .timestamp_ms = ts, .sequence = seq });
                    }
                }
            },
            .cg_create => {
                if (CommandPayload.deserialize(ual_entry.payload)) |cmd| {
                    if (cmd.key.len > 0) {
                        self.createGroup(cmd.key, ual_entry.header.timestamp_ns) catch |err| {
                            if (err != error.AlreadyExists) return err;
                        };
                    }
                }
            },
            .cg_delete => {
                if (CommandPayload.deserialize(ual_entry.payload)) |cmd| {
                    if (cmd.key.len > 0) {
                        _ = self.deleteGroup(cmd.key);
                    }
                }
            },
            else => {},
        }

        self.applied_index = ual_entry.header.index;
    }

    /// ProjectionVTable implementation.
    pub fn projectionHandle(self: *StreamProjection) router_mod.ProjectionHandle {
        return .{
            .ctx = @ptrCast(self),
            .vtable = .{
                .applyFn = vtableApply,
                .memoryUsageFn = vtableMemory,
            },
        };
    }

    fn vtableApply(ctx: *anyopaque, ual_entry: *const Entry) router_mod.ApplyError!void {
        const self: *StreamProjection = @ptrCast(@alignCast(ctx));
        self.applyEntry(ual_entry) catch return error.OutOfMemory;
    }

    fn vtableMemory(ctx: *anyopaque) usize {
        const self: *StreamProjection = @ptrCast(@alignCast(ctx));
        return self.memoryUsage();
    }

    pub fn memoryUsage(self: *const StreamProjection) usize {
        var mem: usize = @sizeOf(StreamProjection);

        var git = self.groups.iterator();
        while (git.next()) |kv| {
            mem += kv.value_ptr.name.len;
            mem += kv.value_ptr.members.count() * @sizeOf(Member);
            mem += kv.value_ptr.pel.count() * (@sizeOf(u128) + @sizeOf(PendingEntry));
        }

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

    // ─── Snapshot Serialization ────────────────────────────────────────────

    /// Serialize the full stream projection state.
    /// Format:
    ///   [stream_name_count: u32] then per stream name:
    ///     [name_len: u16][name bytes]
    ///   [metadata_count: u32] then per stream metadata:
    ///     [name_len: u16][name bytes][partition_count: u32]
    ///     [name_hash: u64][retention_age_s: u64][retention_count: u64][retention_bytes: u64]
    ///   [group_count: u32] then per consumer group:
    ///     [name_len: u16][name bytes][pel_count: u32][created_at_ns: u64]
    ///     [member_count: u32] then per member:
    ///       [id_len: u16][id bytes][joined_at_ns: u64][state: u8]
    /// Caller owns returned slice.
    pub fn serialize(self: *StreamProjection, allocator: Allocator) ![]u8 {
        // Calculate total size
        var total_size: usize = 0;

        // Stream names: count(4) + per name(len_u16 + bytes)
        total_size += 4;
        var sn_it = self.stream_names.keyIterator();
        while (sn_it.next()) |key| {
            total_size += 2 + key.len;
        }

        // Stream metadata: count(4) + per entry(len_u16 + bytes + u32 + 4*u64)
        total_size += 4;
        var sm_it = self.stream_metadata.iterator();
        while (sm_it.next()) |kv| {
            total_size += 2 + kv.key_ptr.len + 4 + 8 + 8 + 8 + 8;
        }

        // Groups: count(4) + per group(...)
        total_size += 4;
        var g_it = self.groups.iterator();
        while (g_it.next()) |kv| {
            const group = kv.value_ptr;
            // name_len(2) + name + pel_count(4) + created_at_ns(8) + member_count(4)
            total_size += 2 + group.name.len + 4 + 8 + 4;
            var m_it = group.members.iterator();
            while (m_it.next()) |mkv| {
                // id_len(2) + id + joined_at_ns(8) + state(1)
                total_size += 2 + mkv.value_ptr.id.len + 8 + 1;
            }
        }

        const buf = try allocator.alloc(u8, total_size);
        errdefer allocator.free(buf);
        var offset: usize = 0;

        // Stream names
        sn_it = self.stream_names.keyIterator();
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.stream_names.count()), .little);
        offset += 4;
        while (sn_it.next()) |key| {
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(key.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..key.len], key.*);
            offset += key.len;
        }

        // Stream metadata (includes retention fields)
        sm_it = self.stream_metadata.iterator();
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.stream_metadata.count()), .little);
        offset += 4;
        while (sm_it.next()) |kv| {
            const name = kv.key_ptr.*;
            const meta = kv.value_ptr;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(name.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..name.len], name);
            offset += name.len;
            std.mem.writeInt(u32, buf[offset..][0..4], meta.partition_count, .little);
            offset += 4;
            std.mem.writeInt(u64, buf[offset..][0..8], meta.name_hash, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], meta.retention_age_s, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], meta.retention_count, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], meta.retention_bytes, .little);
            offset += 8;
        }

        // Groups
        g_it = self.groups.iterator();
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.groups.count()), .little);
        offset += 4;
        while (g_it.next()) |kv| {
            const group = kv.value_ptr;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(group.name.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..group.name.len], group.name);
            offset += group.name.len;
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(group.pelCount()), .little);
            offset += 4;
            std.mem.writeInt(u64, buf[offset..][0..8], group.created_at_ns, .little);
            offset += 8;

            // Members
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(group.members.count()), .little);
            offset += 4;
            var m_it = group.members.iterator();
            while (m_it.next()) |mkv| {
                const member = mkv.value_ptr;
                std.mem.writeInt(u16, buf[offset..][0..2], @intCast(member.id.len), .little);
                offset += 2;
                @memcpy(buf[offset..][0..member.id.len], member.id);
                offset += member.id.len;
                std.mem.writeInt(u64, buf[offset..][0..8], member.joined_at_ns, .little);
                offset += 8;
                buf[offset] = @intFromEnum(member.state);
                offset += 1;
            }
        }

        return buf;
    }

    /// Restore stream projection state from serialized bytes.
    /// Clears all existing state before restoring.
    pub fn deserialize(self: *StreamProjection, data: []const u8) !void {
        self.reset();

        if (data.len < 4) return;
        var offset: usize = 0;

        // Stream names
        if (offset + 4 > data.len) return;
        const sn_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var si: u32 = 0;
        while (si < sn_count) : (si += 1) {
            if (offset + 2 > data.len) return;
            const name_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + name_len > data.len) return;
            const name = try self.allocator.dupe(u8, data[offset..][0..name_len]);
            offset += name_len;
            try self.stream_names.put(name, {});
        }

        // Stream metadata (includes retention fields)
        if (offset + 4 > data.len) return;
        const sm_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var mi: u32 = 0;
        while (mi < sm_count) : (mi += 1) {
            if (offset + 2 > data.len) return;
            const name_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + name_len + 4 + 32 > data.len) return;
            const name = try self.allocator.dupe(u8, data[offset..][0..name_len]);
            offset += name_len;
            const partition_count = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;
            const name_hash = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const retention_age_s = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const retention_count = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const retention_bytes = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            try self.stream_metadata.put(name, .{
                .partition_count = partition_count,
                .name_hash = name_hash,
                .retention_age_s = retention_age_s,
                .retention_count = retention_count,
                .retention_bytes = retention_bytes,
            });
        }

        // Consumer groups
        if (offset + 4 > data.len) return;
        const group_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var gi: u32 = 0;
        while (gi < group_count) : (gi += 1) {
            if (offset + 2 > data.len) return;
            const name_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + name_len + 12 > data.len) return;
            const group_name = data[offset..][0..name_len];
            offset += name_len;
            const _pel_count = std.mem.readInt(u32, data[offset..][0..4], .little);
            _ = _pel_count; // PEL is not serialized, rebuilt from UAL replay
            offset += 4;
            const created_at_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;

            var group = try ConsumerGroup.init(self.allocator, group_name, created_at_ns);

            // Members
            if (offset + 4 > data.len) {
                group.deinit();
                return;
            }
            const member_count = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;

            var mj: u32 = 0;
            while (mj < member_count) : (mj += 1) {
                if (offset + 2 > data.len) {
                    group.deinit();
                    return;
                }
                const id_len = std.mem.readInt(u16, data[offset..][0..2], .little);
                offset += 2;
                if (offset + id_len + 9 > data.len) {
                    group.deinit();
                    return;
                }
                const member_id = try self.allocator.dupe(u8, data[offset..][0..id_len]);
                offset += id_len;
                const joined_at_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
                offset += 8;
                const state: MemberState = @enumFromInt(data[offset]);
                offset += 1;

                try group.members.put(member_id, .{
                    .id = member_id,
                    .joined_at_ns = joined_at_ns,
                    .state = state,
                });
            }

            try self.groups.put(group.name, group);
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "stream: consumer group lifecycle" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    // Create group
    try s.createGroup("my-group", 1000);
    try testing.expectEqual(@as(usize, 1), s.groupCount());

    // Duplicate create returns AlreadyExists (handled gracefully)
    try testing.expectError(error.AlreadyExists, s.createGroup("my-group", 2000));

    // Join member
    const joined = try s.joinGroup("my-group", "consumer-1", 3000);
    try testing.expect(joined);

    // Duplicate join returns false
    const joined2 = try s.joinGroup("my-group", "consumer-1", 4000);
    try testing.expect(!joined2);

    // Get group
    const group = s.getGroup("my-group").?;
    try testing.expectEqual(@as(usize, 1), group.memberCount());

    // Leave
    const left = try s.leaveGroup("my-group", "consumer-1");
    try testing.expect(left);
    try testing.expectEqual(@as(usize, 0), group.memberCount());

    // Delete group
    try testing.expect(s.deleteGroup("my-group"));
    try testing.expectEqual(@as(usize, 0), s.groupCount());
}

test "stream: stats tracking" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 42;
    _ = try s.appendToStream(hash, 100, 0);
    _ = try s.appendToStream(hash, 101, 0);
    try s.createGroup("g", 1000);

    try testing.expectEqual(@as(u64, 2), s.stats.appended);
    try testing.expectEqual(@as(u64, 1), s.stats.groups_created);
}

test "stream: memory usage estimate" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    _ = try s.appendToStream(42, 100, 0);
    try s.createGroup("g", 1000);

    try testing.expect(s.memoryUsage() > 0);
}

test "stream: projection handle with router" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    // Stream projection doesn't register with router currently
    // (stream_append routes to .none in the current router).
    // Test the vtable wiring directly:
    const handle = s.projectionHandle();
    try testing.expect(handle.memoryUsage() > 0);
}

test "stream: apply entry for stream_append" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const ual_entry = entry_mod.buildEntry(.stream_append, 0, 1, 1, 1000, "data");
    try s.applyEntry(&ual_entry);

    try testing.expectEqual(@as(u64, 1), s.stats.appended);
}

test "stream: serialize/deserialize round-trip" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    // Register stream names and metadata
    try s.registerStream("events");
    try s.registerStream("logs");
    try s.registerStreamMetadata("events", .{
        .partition_count = 4,
        .name_hash = 42,
        .retention_age_s = 3600,
        .retention_count = 1000,
        .retention_bytes = 0,
    });

    // Append records to a stream
    const hash: u64 = 42;
    _ = try s.appendToStream(hash, 100, 0);
    _ = try s.appendToStream(hash, 101, 0);

    // Create consumer group with members
    try s.createGroup("my-group", 5000);
    _ = try s.joinGroup("my-group", "consumer-1", 6000);
    _ = try s.joinGroup("my-group", "consumer-2", 7000);

    // Serialize
    const data = try s.serialize(testing.allocator);
    defer testing.allocator.free(data);

    // Deserialize into fresh projection
    var s2 = StreamProjection.init(testing.allocator);
    defer s2.deinit();
    try s2.deserialize(data);

    // Verify stream names restored
    try testing.expect(s2.stream_names.contains("events"));
    try testing.expect(s2.stream_names.contains("logs"));
    try testing.expectEqual(@as(usize, 2), s2.streamCount());

    // Verify metadata (including retention)
    const meta = s2.stream_metadata.get("events").?;
    try testing.expectEqual(@as(u32, 4), meta.partition_count);
    try testing.expectEqual(@as(u64, 42), meta.name_hash);
    try testing.expectEqual(@as(u64, 3600), meta.retention_age_s);
    try testing.expectEqual(@as(u64, 1000), meta.retention_count);
    try testing.expectEqual(@as(u64, 0), meta.retention_bytes);

    // Verify consumer group
    const group = s2.getGroup("my-group").?;
    try testing.expectEqual(@as(usize, 2), group.memberCount());
}

test "stream: serialize empty projection" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const data = try s.serialize(testing.allocator);
    defer testing.allocator.free(data);

    var s2 = StreamProjection.init(testing.allocator);
    defer s2.deinit();
    try s2.deserialize(data);
    try testing.expectEqual(@as(usize, 0), s2.streamCount());
}

// ═══════════════════════════════════════════════════════════════════════════════
// PEL + StreamState Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "stream: StreamState append and read" {
    var ss = StreamState.init(42);
    defer ss.deinit(testing.allocator);

    const id1 = try ss.append(testing.allocator, 100, 0);
    const id2 = try ss.append(testing.allocator, 101, 0);
    const id3 = try ss.append(testing.allocator, 102, 1);

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
    var ss = StreamState.init(42);
    defer ss.deinit(testing.allocator);

    const id1 = try ss.append(testing.allocator, 100, 0);
    const id2 = try ss.append(testing.allocator, 101, 0);
    const id3 = try ss.append(testing.allocator, 102, 0);
    const id4 = try ss.append(testing.allocator, 103, 0);

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
    var ss = StreamState.init(42);
    defer ss.deinit(testing.allocator);

    const id1 = try ss.append(testing.allocator, 100, 0);
    const id2 = try ss.append(testing.allocator, 101, 0);
    _ = try ss.append(testing.allocator, 102, 0);

    const trimmed = ss.trim(testing.allocator, id2);
    try testing.expectEqual(@as(u64, 2), trimmed);
    try testing.expectEqual(@as(usize, 1), ss.count());
    try testing.expect(!ss.first_id.eql(id1));
    try testing.expect(!ss.first_id.eql(id2));
}

test "stream: appendToStream creates stream and returns StreamID" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 12345;
    const id1 = try s.appendToStream(hash, 100, 0);
    const id2 = try s.appendToStream(hash, 101, 0);

    // StreamIDs are monotonically increasing
    try testing.expect(id2.greaterThan(id1));

    // Per-stream state recorded
    try testing.expectEqual(@as(usize, 2), s.streamRecordCount(hash));
    try testing.expect(s.streamFirstId(hash).eql(id1));
    try testing.expect(s.streamLastId(hash).eql(id2));
}

test "stream: readStreamRange and readStreamAfter" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 42;
    const id1 = try s.appendToStream(hash, 100, 0);
    _ = try s.appendToStream(hash, 101, 0);
    const id3 = try s.appendToStream(hash, 102, 1);

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
    const n4 = s.readStreamRange(999, StreamID.MIN, StreamID.MAX, null, &buf);
    try testing.expectEqual(@as(usize, 0), n4);
}

test "stream: trimStream" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 42;
    _ = try s.appendToStream(hash, 100, 0);
    const id2 = try s.appendToStream(hash, 101, 0);
    _ = try s.appendToStream(hash, 102, 0);

    const trimmed = s.trimStream(hash, id2);
    try testing.expectEqual(@as(u64, 2), trimmed);
    try testing.expectEqual(@as(usize, 1), s.streamRecordCount(hash));
}

test "stream: groupDeliver adds to PEL" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 42;
    _ = try s.appendToStream(hash, 100, 0);
    _ = try s.appendToStream(hash, 101, 0);
    _ = try s.appendToStream(hash, 102, 0);

    try s.createGroup("mygroup", 1000);
    _ = try s.joinGroup("mygroup", "consumer-1", 1000);

    var buf: [10]StreamRecord = undefined;
    const delivered = try s.groupDeliver("mygroup", hash, "consumer-1", 2, 5000, &buf);
    try testing.expectEqual(@as(usize, 2), delivered);

    // PEL should have 2 entries
    const pel_count = try s.groupPelCount("mygroup");
    try testing.expectEqual(@as(usize, 2), pel_count);

    // Deliver more — should get the remaining 1
    const delivered2 = try s.groupDeliver("mygroup", hash, "consumer-1", 10, 5001, &buf);
    try testing.expectEqual(@as(usize, 1), delivered2);
}

test "stream: groupAck removes from PEL" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 42;
    const id1 = try s.appendToStream(hash, 100, 0);
    const id2 = try s.appendToStream(hash, 101, 0);
    _ = try s.appendToStream(hash, 102, 0);

    try s.createGroup("mygroup", 1000);
    _ = try s.joinGroup("mygroup", "c1", 1000);

    var buf: [10]StreamRecord = undefined;
    _ = try s.groupDeliver("mygroup", hash, "c1", 3, 5000, &buf);
    try testing.expectEqual(@as(usize, 3), try s.groupPelCount("mygroup"));

    // Ack 2 messages
    const ids = [_]StreamID{ id1, id2 };
    const acked = try s.groupAck("mygroup", &ids);
    try testing.expectEqual(@as(u32, 2), acked);
    try testing.expectEqual(@as(usize, 1), try s.groupPelCount("mygroup"));
}

test "stream: groupNack marks for redelivery" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 42;
    const id1 = try s.appendToStream(hash, 100, 0);

    try s.createGroup("mygroup", 1000);
    _ = try s.joinGroup("mygroup", "c1", 1000);

    var buf: [10]StreamRecord = undefined;
    _ = try s.groupDeliver("mygroup", hash, "c1", 1, 5000, &buf);

    // Nack
    const ids = [_]StreamID{id1};
    const nacked = try s.groupNack("mygroup", &ids, 6000);
    try testing.expectEqual(@as(u32, 1), nacked);

    // Still in PEL (delivery_count incremented)
    try testing.expectEqual(@as(usize, 1), try s.groupPelCount("mygroup"));

    // Verify delivery count via getPending
    var pel_buf: [10]PendingEntry = undefined;
    const n = try s.groupPending("mygroup", null, &pel_buf);
    try testing.expectEqual(@as(usize, 1), n);
    try testing.expectEqual(@as(u32, 2), pel_buf[0].delivery_count);
}

test "stream: groupClaim transfers ownership" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 42;
    const id1 = try s.appendToStream(hash, 100, 0);

    try s.createGroup("mygroup", 1000);
    _ = try s.joinGroup("mygroup", "c1", 1000);
    _ = try s.joinGroup("mygroup", "c2", 1000);

    var buf: [10]StreamRecord = undefined;
    _ = try s.groupDeliver("mygroup", hash, "c1", 1, 5000, &buf);

    // Claim with 0 min_idle so it always matches
    const ids = [_]StreamID{id1};
    const claimed = try s.groupClaim("mygroup", &ids, "c2", 0, 6000);
    try testing.expectEqual(@as(u32, 1), claimed);

    // Verify consumer changed via getPending
    var pel_buf: [10]PendingEntry = undefined;
    const n = try s.groupPending("mygroup", "c2", &pel_buf);
    try testing.expectEqual(@as(usize, 1), n);
}

test "stream: sweeper re-nacks idle entries then drops poison (FLO-102)" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 42;
    _ = try s.appendToStream(hash, 100, 0);

    try s.createGroup("g", 1000);
    _ = try s.joinGroup("g", "c1", 1000);
    // Tight config: re-nack after 200ms idle, drop after 3 deliveries.
    try s.configureGroup("g", 200, 3);

    var rec_buf: [10]StreamRecord = undefined;
    _ = try s.groupDeliver("g", hash, "c1", 1, 1000, &rec_buf); // delivery_count=1, last=1000

    // Not yet idle enough → no-op.
    var r = s.sweepAllGroups(1100);
    try testing.expectEqual(@as(u32, 0), r.renacked);
    try testing.expectEqual(@as(u32, 0), r.dropped);

    // Idle ≥ 200ms → re-nacked (delivery_count 1→2), still in PEL.
    r = s.sweepAllGroups(1400);
    try testing.expectEqual(@as(u32, 1), r.renacked);
    try testing.expectEqual(@as(usize, 1), try s.groupPelCount("g"));

    // Re-nack again (2→3). Now delivery_count == max_deliver (3).
    r = s.sweepAllGroups(1700);
    try testing.expectEqual(@as(u32, 1), r.renacked);

    // Next sweep: delivery_count (3) >= max_deliver (3) → dropped from PEL.
    r = s.sweepAllGroups(2000);
    try testing.expectEqual(@as(u32, 1), r.dropped);
    try testing.expectEqual(@as(usize, 0), try s.groupPelCount("g"));
}

test "stream: sweeper disabled when ack_timeout_ms is 0 (FLO-102)" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 7;
    _ = try s.appendToStream(hash, 100, 0);
    try s.createGroup("g", 1000);
    _ = try s.joinGroup("g", "c1", 1000);
    try s.configureGroup("g", 0, 10); // ack_timeout_ms = 0 → sweeper off

    var rec_buf: [10]StreamRecord = undefined;
    _ = try s.groupDeliver("g", hash, "c1", 1, 1000, &rec_buf);

    const r = s.sweepAllGroups(1_000_000);
    try testing.expectEqual(@as(u32, 0), r.renacked);
    try testing.expectEqual(@as(u32, 0), r.dropped);
    try testing.expectEqual(@as(usize, 1), try s.groupPelCount("g"));
}

test "stream: groupAutoclaim auto-transfers idle entries" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 42;
    _ = try s.appendToStream(hash, 100, 0);
    _ = try s.appendToStream(hash, 101, 0);

    try s.createGroup("mygroup", 1000);
    _ = try s.joinGroup("mygroup", "c1", 1000);
    _ = try s.joinGroup("mygroup", "c2", 1000);

    var rec_buf: [10]StreamRecord = undefined;
    _ = try s.groupDeliver("mygroup", hash, "c1", 2, 1000, &rec_buf);

    // Autoclaim after enough idle time (entries delivered at t=1000, now=5000, idle=4000ms)
    var out: [10]StreamID = undefined;
    const res = try s.groupAutoclaim("mygroup", "c2", 3000, 5000, StreamID.MIN, 10, &out);
    try testing.expectEqual(@as(u32, 2), res.count);
    // Fully scanned → cursor is MAX
    try testing.expect(res.next_cursor.eql(StreamID.MAX));

    // Verify ownership transferred to c2
    var pel_buf: [10]PendingEntry = undefined;
    try testing.expectEqual(@as(usize, 2), try s.groupPending("mygroup", "c2", &pel_buf));
}

test "stream: groupAutoclaim drains own pending (reconnect) and paginates" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 77;
    _ = try s.appendToStream(hash, 100, 0);
    _ = try s.appendToStream(hash, 101, 0);
    _ = try s.appendToStream(hash, 102, 0);

    try s.createGroup("g", 1000);
    _ = try s.joinGroup("g", "me", 1000);

    var rec_buf: [10]StreamRecord = undefined;
    _ = try s.groupDeliver("g", hash, "me", 3, 1000, &rec_buf);

    // Reconnect drain: min_idle_ms=0, same consumer — must return own entries
    // (the old impl skipped same-owner and would return 0).
    var out: [10]StreamID = undefined;
    const page1 = try s.groupAutoclaim("g", "me", 0, 2000, StreamID.MIN, 2, &out);
    try testing.expectEqual(@as(u32, 2), page1.count);
    // Batch full at 2 < 3 entries → cursor points at the 3rd entry, not MAX
    try testing.expect(!page1.next_cursor.eql(StreamID.MAX));

    // Second page from the returned cursor picks up the remaining entry.
    const page2 = try s.groupAutoclaim("g", "me", 0, 2000, page1.next_cursor, 10, &out);
    try testing.expectEqual(@as(u32, 1), page2.count);
    try testing.expect(page2.next_cursor.eql(StreamID.MAX));
}

test "stream: groupTouch resets idle timer" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash: u64 = 42;
    const id1 = try s.appendToStream(hash, 100, 0);

    try s.createGroup("mygroup", 1000);
    _ = try s.joinGroup("mygroup", "c1", 1000);

    var buf: [10]StreamRecord = undefined;
    _ = try s.groupDeliver("mygroup", hash, "c1", 1, 1000, &buf);

    // Touch at t=5000
    const ids = [_]StreamID{id1};
    const touched = try s.groupTouch("mygroup", &ids, 5000);
    try testing.expectEqual(@as(u32, 1), touched);

    // Verify last_delivery_ms updated via getPending
    var pel_buf: [10]PendingEntry = undefined;
    _ = try s.groupPending("mygroup", null, &pel_buf);
    try testing.expectEqual(@as(u64, 5000), pel_buf[0].last_delivery_ms);
}

test "stream: groupDeliver to non-existent group" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    var buf: [10]StreamRecord = undefined;
    try testing.expectError(error.GroupNotFound, s.groupDeliver("nope", 42, "c1", 1, 1000, &buf));
}

test "stream: multiple streams are independent" {
    var s = StreamProjection.init(testing.allocator);
    defer s.deinit();

    const hash_a: u64 = 1;
    const hash_b: u64 = 2;

    const a1 = try s.appendToStream(hash_a, 100, 0);
    const a2 = try s.appendToStream(hash_a, 101, 0);
    _ = try s.appendToStream(hash_b, 200, 0);

    try testing.expectEqual(@as(usize, 2), s.streamRecordCount(hash_a));
    try testing.expectEqual(@as(usize, 1), s.streamRecordCount(hash_b));

    // Reading stream A returns only A's records
    var buf: [10]StreamRecord = undefined;
    const n = s.readStreamAfter(hash_a, a1, null, &buf);
    try testing.expectEqual(@as(usize, 1), n);
    try testing.expect(buf[0].id.eql(a2));
}
