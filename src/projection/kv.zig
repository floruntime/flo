//! KV Projection — in-memory hash table with MVCC versioning.
//!
//! Maintains a hash map of key → KVEntry for O(1) point lookups.
//! Supports put, get, delete, scan, and TTL expiry.
//! Consumer group state is also stored here as regular KV entries.
//!
//! Each key keeps its older versions, bounded by count and by bytes
//! (kv_ops.HISTORY_*): the oldest go first.
//!
//! applied via ProjectionRouter from committed UAL entries:
//!   kv_put    → insert or update key
//!   kv_delete → tombstone the key
//!   kv_batch  → apply multiple ops atomically
//!   cg_*      → consumer group state (stored as KV with cg: prefix)

const std = @import("std");
const Allocator = std.mem.Allocator;
const entry_mod = @import("../storage/ual/entry.zig");
const router_mod = @import("router.zig");
const time_units = @import("../util/time_units.zig");
const schema = @import("../protocol/schema.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const kv_ops = @import("../protocol/kv_ops.zig");

// Each KV entry's code is its UAL type's: `kv.put` is `kv_put`, so a
// renumbering on either side fails the build instead of mis-mapping.
comptime {
    for (kv_entries.table) |e| {
        const tag = "kv_" ++ e.name["kv.".len..];
        if (@backingInt(@field(entry_mod.EntryType, tag)) != e.type_code) @compileError("KV entry '" ++ e.name ++ "' has code " ++ std.fmt.comptimePrint("{d}", .{e.type_code}) ++ ", but " ++ tag ++ " is another");
    }
}

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
pub const Result = router_mod.Result;

// ═══════════════════════════════════════════════════════════════════════════════
// KV Entry — stored in the hash map
// ═══════════════════════════════════════════════════════════════════════════════

/// Logical type of a stored value. Used to distinguish opaque strings from
/// integer counters (which support atomic INCR/DECR semantics) and JSON
/// documents (which are still stored as strings but are conceptually
/// structured).
pub const ValueType = enum(u8) {
    string = 0,
    counter = 1,
    json = 2,
};

pub const KVEntry = struct {
    /// The key (owned, allocated).
    key: []const u8,
    /// The value (owned, allocated). Empty slice for tombstones.
    value: []const u8,
    /// UAL index that last modified this entry.
    lsn: u64,
    /// Per-key monotonic version (1 on first write, incremented on each update/delete).
    version: u64,
    /// Raft term of the write.
    term: u64,
    /// Wall clock timestamp (nanoseconds) of the write.
    timestamp_ns: u64,
    /// TTL expiry (nanoseconds). 0 = no expiry.
    expiry_ns: u64,
    /// Whether this entry is a tombstone (deleted).
    tombstone: bool,
    /// Logical value type.
    value_type: ValueType = .string,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Scan Result
// ═══════════════════════════════════════════════════════════════════════════════

pub const ScanEntry = struct {
    key: []const u8,
    value: []const u8,
    lsn: u64,
};

// ═══════════════════════════════════════════════════════════════════════════════
// KV Projection
// ═══════════════════════════════════════════════════════════════════════════════

/// A historical version entry — stores previous value and metadata.
pub const VersionEntry = struct {
    value: []const u8,
    version: u64,
    lsn: u64,
    term: u64,
    timestamp_ns: u64,
    tombstone: bool,

    /// What it counts against its key's history bytes.
    fn charge(v: VersionEntry) u64 {
        return v.value.len + kv_ops.HISTORY_ENTRY_COST;
    }
};

comptime {
    if (kv_ops.HISTORY_ENTRY_COST < @sizeOf(VersionEntry)) @compileError("HISTORY_ENTRY_COST must cover a VersionEntry");
}

/// A key's older versions, oldest first, and what they are charged.
const Chain = struct {
    items: std.ArrayListUnmanaged(VersionEntry) = .empty,
    bytes: u64 = 0,
};

pub const KVProjection = struct {
    allocator: Allocator,
    /// Main hash map: key → KVEntry.
    map: std.StringHashMap(KVEntry),
    /// Version chains: key → its older versions, keyed by the map entry's
    /// own key bytes.
    version_chains: std.StringHashMap(Chain),
    /// Last applied UAL index.
    applied_index: u64,

    /// Stats.
    stats: Stats,
    /// What "now" is when a read judges expiry.
    read_clock: ReadClock = .{},
    /// Per namespace: its live keys, the bytes it stores and its history,
    /// changed only in apply and in `removeKey`, so every replica and every
    /// restart counts the same. A key past its TTL counts until it is
    /// deleted, overwritten or removed: nothing removes it before then.
    live_keys: std.StringHashMapUnmanaged(LiveKeys) = .empty,
    /// Some namespace's counts changed since the shard last published.
    live_keys_changed: bool = false,

    pub const LiveKeys = struct {
        counts: Counts = .{},
        /// What the shard last published for this namespace.
        published: Counts = .{},
        /// Where the shard publishes it, once it has looked it up; the
        /// projection never reads it.
        publish_to: ?*anyopaque = null,

        pub const Counts = struct {
            /// Keys written and not deleted.
            live: u64 = 0,
            /// Key and current value bytes, tombstones and expired values
            /// included; history excluded.
            bytes: u64 = 0,
            /// Older versions' charge: value bytes plus HISTORY_ENTRY_COST each.
            history_bytes: u64 = 0,
            /// Older versions evicted by either bound. Replay evicts them
            /// again, so it counts from this process's start.
            history_evicted: u64 = 0,
        };
    };

    /// The shard points this at its Raft node, so a read judges expiry by
    /// the same time line the entry stamps follow: a key a replica has
    /// already treated as expired at apply never reads back as live. The
    /// default, the wall clock, is for a projection on its own.
    pub const ReadClock = struct {
        ctx: ?*const anyopaque = null,
        now_ns: *const fn (?*const anyopaque) u64 = wallNs,

        fn wallNs(_: ?*const anyopaque) u64 {
            return @intCast(@max(0, @import("stdx").time.nanoTimestamp()));
        }
    };

    fn readNow(self: *const KVProjection) u64 {
        return self.read_clock.now_ns(self.read_clock.ctx);
    }

    /// Whether `entry` has a TTL that had run out by `at_ns`.
    fn expiredAt(entry: *const KVEntry, at_ns: u64) bool {
        return entry.expiry_ns > 0 and entry.expiry_ns <= at_ns;
    }

    pub const Stats = struct {
        puts: u64 = 0,
        deletes: u64 = 0,
        gets: u64 = 0,
        scans: u64 = 0,
        expired: u64 = 0,
    };

    pub fn init(allocator: Allocator) KVProjection {
        return .{
            .allocator = allocator,
            .map = std.StringHashMap(KVEntry).init(allocator),
            .version_chains = std.StringHashMap(Chain).init(allocator),
            .applied_index = 0,
            .stats = .{},
        };
    }

    pub fn deinit(self: *KVProjection) void {
        var it = self.map.iterator();
        while (it.next()) |kv| {
            self.freeEntry(kv.value_ptr);
        }
        self.map.deinit();

        // freeEntry took every key's chain.
        std.debug.assert(self.version_chains.count() == 0);
        self.version_chains.deinit();

        var ns_it = self.live_keys.keyIterator();
        while (ns_it.next()) |ns| self.allocator.free(ns.*);
        self.live_keys.deinit(self.allocator);
    }

    /// The namespace a stored key belongs to: what precedes its separator,
    /// or "default" when it has none. Read off the key, so "" and
    /// "default" can't count apart.
    pub fn namespaceOf(key: []const u8) []const u8 {
        const sep = std.mem.indexOfScalar(u8, key, 0) orelse return "default";
        return key[0..sep];
    }

    /// `key`'s namespace count, made if it is the first. Taken before the
    /// key changes, so a failed allocation leaves the count and the key
    /// agreeing.
    fn liveCounter(self: *KVProjection, key: []const u8) !*LiveKeys {
        const ns = namespaceOf(key);
        const gop = try self.live_keys.getOrPut(self.allocator, ns);
        if (!gop.found_existing) {
            gop.key_ptr.* = self.allocator.dupe(u8, ns) catch |err| {
                self.live_keys.removeByPtr(gop.key_ptr);
                return err;
            };
            gop.value_ptr.* = .{};
        }
        return gop.value_ptr;
    }

    fn noteLive(self: *KVProjection, counter: *LiveKeys) void {
        counter.counts.live += 1;
        self.live_keys_changed = true;
    }

    /// A live key in `counter`'s namespace became a tombstone or went.
    fn noteGone(self: *KVProjection, counter: *LiveKeys) void {
        counter.counts.live -|= 1;
        self.live_keys_changed = true;
    }

    /// Live keys in `namespace` on this projection.
    pub fn liveKeys(self: *const KVProjection, namespace: []const u8) u64 {
        return if (self.live_keys.get(namespace)) |c| c.counts.live else 0;
    }

    /// `namespace`'s counts on this projection.
    pub fn namespaceCounts(self: *const KVProjection, namespace: []const u8) LiveKeys.Counts {
        return if (self.live_keys.get(namespace)) |c| c.counts else .{};
    }

    /// Number of live (non-tombstone) entries.
    pub fn count(self: *const KVProjection) usize {
        var live: usize = 0;
        var it = self.map.iterator();
        while (it.next()) |kv| {
            if (!kv.value_ptr.tombstone) live += 1;
        }
        return live;
    }

    /// Total entries including tombstones.
    pub fn totalEntries(self: *const KVProjection) usize {
        return self.map.count();
    }

    // ─── Point operations ──────────────────────────────────────────────────

    /// Put a key-value pair. expiry_ns = 0 means no expiration.
    pub fn put(self: *KVProjection, key: []const u8, value: []const u8, lsn: u64, term: u64, timestamp_ns: u64, expiry_ns: u64) !void {
        return self.putWithType(key, value, lsn, term, timestamp_ns, expiry_ns, .string);
    }

    /// Put a key-value pair, specifying the logical value type. Every step
    /// that can fail comes before the first change, so an allocation
    /// failure leaves the key, its history and every count as they were.
    pub fn putWithType(self: *KVProjection, key: []const u8, value: []const u8, lsn: u64, term: u64, timestamp_ns: u64, expiry_ns: u64, value_type: ValueType) !void {
        const counter = try self.liveCounter(key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);

        if (self.map.getPtr(key)) |existing| {
            const chain = try self.reserveHistory(existing.key);
            // Nothing below fails.
            if (existing.tombstone) self.noteLive(counter);
            self.pushVersion(chain, existing, counter);
            existing.value = owned_value;
            existing.lsn = lsn;
            existing.version += 1;
            existing.term = term;
            existing.timestamp_ns = timestamp_ns;
            existing.expiry_ns = expiry_ns;
            existing.tombstone = false;
            existing.value_type = value_type;
            counter.counts.bytes += owned_value.len;
        } else {
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.map.put(owned_key, .{
                .key = owned_key,
                .value = owned_value,
                .lsn = lsn,
                .version = 1,
                .term = term,
                .timestamp_ns = timestamp_ns,
                .expiry_ns = expiry_ns,
                .tombstone = false,
                .value_type = value_type,
            });
            self.noteLive(counter);
            counter.counts.bytes += owned_key.len + owned_value.len;
        }
        self.live_keys_changed = true;
        self.stats.puts += 1;
    }

    /// Get a value by key. Returns null if not found or tombstoned.
    pub fn get(self: *KVProjection, key: []const u8) ?*const KVEntry {
        self.stats.gets += 1;
        const entry = self.map.getPtr(key) orelse return null;
        if (entry.tombstone) return null;
        // Check TTL
        if (expiredAt(entry, self.readNow())) {
            // Lazily expire — don't remove yet, just return null
            return null;
        }
        return entry;
    }

    /// Get raw entry (including tombstones) for internal use.
    pub fn getRaw(self: *KVProjection, key: []const u8) ?*const KVEntry {
        return self.map.getPtr(key);
    }

    /// Delete a key (mark as tombstone). Its history stays: a tombstone
    /// keeps its chain until the key is removed. As in `putWithType`, every
    /// step that can fail comes first.
    pub fn delete(self: *KVProjection, key: []const u8, lsn: u64, term: u64, timestamp_ns: u64) !void {
        const counter = try self.liveCounter(key);
        if (self.map.getPtr(key)) |existing| {
            const chain = try self.reserveHistory(existing.key);
            // Nothing below fails.
            if (!existing.tombstone) self.noteGone(counter);
            self.pushVersion(chain, existing, counter);
            existing.value = "";
            existing.lsn = lsn;
            existing.version += 1;
            existing.term = term;
            existing.timestamp_ns = timestamp_ns;
            existing.tombstone = true;
        } else {
            // A tombstone for a key this projection never saw.
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.map.put(owned_key, .{
                .key = owned_key,
                .value = "",
                .lsn = lsn,
                .version = 1,
                .term = term,
                .timestamp_ns = timestamp_ns,
                .expiry_ns = 0,
                .tombstone = true,
            });
            counter.counts.bytes += owned_key.len;
        }
        self.live_keys_changed = true;
        self.stats.deletes += 1;
    }

    // ─── Atomic counters ──────────────────────────────────────────────────

    /// Apply an atomic increment by `delta` to `key`. If the key does not
    /// exist, it is created with value `delta`. If the key exists, its value
    /// is parsed as an i64 (little-endian, 8 bytes) and incremented. Returns
    /// the new value. Returns error.NotACounter if the existing value is not
    /// the expected width, and error.Overflow on signed-integer overflow.
    pub fn applyIncr(self: *KVProjection, key: []const u8, delta: i64, lsn: u64, term: u64, timestamp_ns: u64) !i64 {
        // A counter whose TTL ran out by this entry's stamp starts again
        // from zero, on every replica alike.
        var current: i64 = 0;
        if (self.map.getPtr(key)) |existing| {
            if (!existing.tombstone and !expiredAt(existing, timestamp_ns)) {
                if (existing.value.len != 8) return error.NotACounter;
                current = std.mem.readInt(i64, existing.value[0..8], .little);
            }
        }

        const result = std.math.add(i64, current, delta) catch return error.Overflow;
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, result, .little);
        try self.putWithType(key, &buf, lsn, term, timestamp_ns, 0, .counter);
        return result;
    }

    // ─── TTL lifecycle ─────────────────────────────────────────────────────

    /// Update the TTL of an existing key without rewriting the value.
    /// `expiry_ns = 0` clears the TTL (PERSIST). Updates lsn/term/timestamp_ns
    /// but leaves `version` unchanged (TTL changes are not value mutations).
    /// Returns error.NotFound if the key does not exist, is tombstoned, or
    /// its TTL ran out by `timestamp_ns`.
    pub fn applyTouch(self: *KVProjection, key: []const u8, expiry_ns: u64, lsn: u64, term: u64, timestamp_ns: u64) !void {
        const existing = self.map.getPtr(key) orelse return error.NotFound;
        if (existing.tombstone or expiredAt(existing, timestamp_ns)) return error.NotFound;
        existing.expiry_ns = expiry_ns;
        existing.lsn = lsn;
        existing.term = term;
        existing.timestamp_ns = timestamp_ns;
    }

    /// Scan all live (non-tombstone) entries. Results are unsorted.
    /// Caller provides a bounded output buffer.
    pub fn scan(self: *KVProjection, out: []ScanEntry) usize {
        self.stats.scans += 1;
        const now = self.readNow();
        var count_written: usize = 0;
        var it = self.map.iterator();
        while (it.next()) |kv| {
            if (count_written >= out.len) break;
            const entry = kv.value_ptr;
            if (entry.tombstone) continue;
            if (expiredAt(entry, now)) continue;
            out[count_written] = .{
                .key = entry.key,
                .value = entry.value,
                .lsn = entry.lsn,
            };
            count_written += 1;
        }
        return count_written;
    }

    /// Scan entries matching a key prefix.
    pub fn scanPrefix(self: *KVProjection, prefix: []const u8, out: []ScanEntry) usize {
        self.stats.scans += 1;
        const now = self.readNow();
        var count_written: usize = 0;
        var it = self.map.iterator();
        while (it.next()) |kv| {
            if (count_written >= out.len) break;
            const entry = kv.value_ptr;
            if (entry.tombstone) continue;
            if (expiredAt(entry, now)) continue;
            if (entry.key.len >= prefix.len and
                std.mem.eql(u8, entry.key[0..prefix.len], prefix))
            {
                out[count_written] = .{
                    .key = entry.key,
                    .value = entry.value,
                    .lsn = entry.lsn,
                };
                count_written += 1;
            }
        }
        return count_written;
    }

    /// Scan key names only (zero-allocation). Returns borrowed references
    /// to HashMap key storage. Filters tombstones and expired entries.
    /// Applies optional prefix matching. Caller must not mutate the
    /// projection while references are alive (guaranteed by single-threaded shard).
    pub fn scanKeyNames(self: *KVProjection, prefix: []const u8, out: [][]const u8) usize {
        self.stats.scans += 1;
        const now = self.readNow();
        var n_found: usize = 0;
        var it = self.map.iterator();
        while (it.next()) |kv| {
            if (n_found >= out.len) break;
            const entry = kv.value_ptr;
            if (entry.tombstone) continue;
            if (expiredAt(entry, now)) continue;
            if (prefix.len > 0) {
                if (entry.key.len < prefix.len or
                    !std.mem.eql(u8, entry.key[0..prefix.len], prefix)) continue;
            }
            out[n_found] = entry.key;
            n_found += 1;
        }
        return n_found;
    }

    /// Remove all entries whose key starts with `prefix`, history and all.
    /// Used for namespace force-delete: pass "namespace\x00" to clear all keys in that namespace.
    /// Returns the number of entries removed.
    pub fn clearByPrefix(self: *KVProjection, prefix: []const u8) usize {
        // Collect keys to remove (can't modify while iterating)
        var to_remove_buf: [1024][]const u8 = undefined;
        var total_removed: usize = 0;

        // May need multiple passes if > 1024 keys match
        while (true) {
            var remove_count: usize = 0;
            var it = self.map.iterator();
            while (it.next()) |kv| {
                if (remove_count >= to_remove_buf.len) break;
                const entry_key = kv.value_ptr.key;
                if (entry_key.len >= prefix.len and
                    std.mem.eql(u8, entry_key[0..prefix.len], prefix))
                {
                    to_remove_buf[remove_count] = entry_key;
                    remove_count += 1;
                }
            }

            if (remove_count == 0) break;

            for (to_remove_buf[0..remove_count]) |key| self.removeKey(key);
            total_removed += remove_count;
        }

        return total_removed;
    }

    /// Remove `key` from the map with its history, freeing both and taking
    /// them out of its namespace's counts. The one way a key leaves the map.
    pub fn removeKey(self: *KVProjection, key: []const u8) void {
        const removed = self.map.fetchRemove(key) orelse return;
        var entry = removed.value;
        if (self.live_keys.getPtr(namespaceOf(entry.key))) |counter| {
            if (!entry.tombstone) self.noteGone(counter);
            counter.counts.bytes -|= entry.key.len + entry.value.len;
            if (self.version_chains.get(entry.key)) |chain| counter.counts.history_bytes -|= chain.bytes;
            self.live_keys_changed = true;
        }
        self.freeEntry(&entry);
    }

    // ─── Version Chain (MVCC History) ──────────────────────────────────────

    /// `key`'s chain, with room for one more version, so the push that
    /// follows can't fail. `key` is the map entry's own key bytes.
    fn reserveHistory(self: *KVProjection, key: []const u8) !*Chain {
        const gop = try self.version_chains.getOrPut(key);
        if (!gop.found_existing) gop.value_ptr.* = .{};
        // A failure leaves the chains as they were, not with an empty one.
        errdefer if (!gop.found_existing) self.version_chains.removeByPtr(gop.key_ptr);
        try gop.value_ptr.items.ensureUnusedCapacity(self.allocator, 1);
        return gop.value_ptr;
    }

    /// Move `existing`'s current state onto `chain`, which `reserveHistory`
    /// made room in, then evict the oldest versions while either bound is
    /// exceeded, keeping the newest. The caller overwrites `existing` next.
    fn pushVersion(self: *KVProjection, chain: *Chain, existing: *const KVEntry, counter: *LiveKeys) void {
        const version = VersionEntry{
            // A tombstone's "" isn't owned; a live value moves here.
            .value = if (existing.tombstone) "" else existing.value,
            .version = existing.version,
            .lsn = existing.lsn,
            .term = existing.term,
            .timestamp_ns = existing.timestamp_ns,
            .tombstone = existing.tombstone,
        };
        chain.items.appendAssumeCapacity(version);
        chain.bytes += version.charge();
        counter.counts.bytes -|= version.value.len;
        counter.counts.history_bytes += version.charge();

        var drop: usize = 0;
        const items = chain.items.items;
        while (items.len - drop > 1 and
            (items.len - drop > kv_ops.HISTORY_MAX_VERSIONS or chain.bytes > kv_ops.HISTORY_MAX_BYTES)) : (drop += 1)
        {
            const old = items[drop];
            chain.bytes -= old.charge();
            counter.counts.history_bytes -|= old.charge();
            if (old.value.len > 0) self.allocator.free(@constCast(old.value));
        }
        if (drop > 0) {
            std.mem.copyForwards(VersionEntry, items[0 .. items.len - drop], items[drop..]);
            chain.items.shrinkRetainingCapacity(items.len - drop);
            counter.counts.history_evicted += drop;
        }
    }

    /// A key's versions, newest first, from below `before` (exclusive)
    /// when given. It borrows the projection's memory: valid only on the
    /// shard thread within one handler call, and any apply invalidates it.
    pub const History = struct {
        current: ?*const KVEntry,
        chain: []const VersionEntry,
        before: ?u64,
        /// Next chain index to give, counting down; the current comes first.
        next_index: usize,
        gave_current: bool = false,

        /// Whether the key has any history at all.
        pub fn exists(h: *const History) bool {
            return h.current != null or h.chain.len > 0;
        }

        pub fn next(h: *History) ?VersionEntry {
            if (!h.gave_current) {
                h.gave_current = true;
                if (h.current) |e| if (h.before == null or e.version < h.before.?) return .{
                    .value = e.value,
                    .version = e.version,
                    .lsn = e.lsn,
                    .term = e.term,
                    .timestamp_ns = e.timestamp_ns,
                    .tombstone = e.tombstone,
                };
            }
            while (h.next_index > 0) {
                h.next_index -= 1;
                const v = h.chain[h.next_index];
                if (h.before == null or v.version < h.before.?) return v;
            }
            return null;
        }
    };

    pub fn history(self: *KVProjection, key: []const u8, before: ?u64) History {
        const chain: []const VersionEntry = if (self.version_chains.getPtr(key)) |c| c.items.items else &.{};
        return .{ .current = self.map.getPtr(key), .chain = chain, .before = before, .next_index = chain.len };
    }

    /// Remove all tombstones, history and all. Returns number purged.
    pub fn compact(self: *KVProjection) usize {
        var to_remove: [256][]const u8 = undefined;
        var remove_count: usize = 0;

        var it = self.map.iterator();
        while (it.next()) |kv| {
            if (remove_count >= to_remove.len) break;
            const entry = kv.value_ptr;
            if (entry.tombstone) {
                to_remove[remove_count] = entry.key;
                remove_count += 1;
            }
        }

        for (to_remove[0..remove_count]) |key| self.removeKey(key);
        return remove_count;
    }

    // ─── UAL Entry application (ProjectionRouter interface) ────────────────

    /// Apply a committed UAL entry to this projection, recording what the
    /// client is answered in `out`. A payload that doesn't decode whole is a
    /// bug in the proposer: nothing of it is applied, and it halts. An incr
    /// that overflows or meets a non-counter, and an expire of a key that's
    /// gone, change nothing and are refused: every replica reaches the same
    /// state and the same answer. Inside a batch they change nothing too,
    /// and the rest of the batch applies.
    ///
    /// Answers: put, the key's version; delete, whether the key was there
    /// (u8); incr, its version and value; expire, nothing; batch, its op
    /// count (u16).
    pub fn applyEntry(self: *KVProjection, entry: *const Entry, out: *Result) router_mod.ApplyError!void {
        const entry_type: EntryType = @fromBackingInt(@intCast(entry.header.entry_type));
        const h = entry.header;
        var diag: schema.Diagnostic = .{};

        switch (entry_type) {
            .kv_put => {
                const p = kv_entries.Put.decodeAll(entry.payload, &diag) catch return error.Halt;
                try self.put(p.key, p.value, h.index, h.term, h.timestamp_ns, time_units.expiryAfter(h.timestamp_ns, p.ttl_ms orelse 0));
                out.putInt(u64, self.map.getPtr(p.key).?.version);
            },
            .kv_delete => {
                const p = kv_entries.Delete.decodeAll(entry.payload, &diag) catch return error.Halt;
                const existed = if (self.map.getPtr(p.key)) |e| !e.tombstone and !expiredAt(e, h.timestamp_ns) else false;
                try self.delete(p.key, h.index, h.term, h.timestamp_ns);
                out.putInt(u8, @intFromBool(existed));
            },
            .kv_incr => {
                const p = kv_entries.Incr.decodeAll(entry.payload, &diag) catch return error.Halt;
                const value = self.applyIncr(p.key, p.delta, h.index, h.term, h.timestamp_ns) catch |err| switch (err) {
                    error.Overflow => return out.refuse(.bad_request, .out_of_range, INCR_OVERFLOW),
                    error.NotACounter => return out.refuse(.bad_request, .wrong_type, NOT_A_COUNTER),
                    error.OutOfMemory => return error.OutOfMemory,
                };
                out.putInt(u64, self.map.getPtr(p.key).?.version);
                out.putInt(i64, value);
            },
            .kv_expire => {
                const p = kv_entries.Expire.decodeAll(entry.payload, &diag) catch return error.Halt;
                self.applyTouch(p.key, time_units.expiryAfter(h.timestamp_ns, p.ttl_ms), h.index, h.term, h.timestamp_ns) catch |err| switch (err) {
                    error.NotFound => return out.refuse(.not_found, .not_found, "not found"),
                };
            },
            .kv_batch => {
                // Decoded and checked whole before any op applies, so a bad
                // batch changes nothing.
                const batch = kv_entries.Batch.decodeAll(entry.payload, &diag) catch return error.Halt;
                var check = batch.ops.iterator();
                var ops: u16 = 0;
                while (check.next() catch return error.Halt) |op| {
                    if (kv_entries.batchOpRule(op) != null) return error.Halt;
                    ops += 1;
                }
                var it = batch.ops.iterator();
                while (it.next() catch return error.Halt) |op| switch (op.kind) {
                    .put => try self.put(op.key, op.value.?, h.index, h.term, h.timestamp_ns, time_units.expiryAfter(h.timestamp_ns, op.ttl_ms orelse 0)),
                    .delete => try self.delete(op.key, h.index, h.term, h.timestamp_ns),
                    .incr => _ = self.applyIncr(op.key, op.delta.?, h.index, h.term, h.timestamp_ns) catch |err| switch (err) {
                        error.Overflow, error.NotACounter => {},
                        error.OutOfMemory => return error.OutOfMemory,
                    },
                    .expire => self.applyTouch(op.key, time_units.expiryAfter(h.timestamp_ns, op.ttl_ms.?), h.index, h.term, h.timestamp_ns) catch |err| switch (err) {
                        error.NotFound => {},
                    },
                };
                out.putInt(u16, ops);
            },
            else => return error.Halt,
        }

        self.applied_index = entry.header.index;
    }

    pub const INCR_OVERFLOW = "bad request: delta would take the counter past an i64";
    pub const NOT_A_COUNTER = "bad request: the key's value isn't an 8-byte counter";

    /// ProjectionVTable implementation for use with ProjectionRouter.
    pub fn projectionHandle(self: *KVProjection) router_mod.ProjectionHandle {
        return .{
            .ctx = @ptrCast(self),
            .vtable = .{ .applyFn = vtableApply },
        };
    }

    fn vtableApply(ctx: *anyopaque, entry: *const Entry, out: *Result) router_mod.ApplyError!void {
        const self: *KVProjection = @ptrCast(@alignCast(ctx));
        return self.applyEntry(entry, out);
    }

    // ─── Internal ──────────────────────────────────────────────────────────

    fn freeEntry(self: *KVProjection, entry: *KVEntry) void {
        // Free version chain for this key
        if (self.version_chains.fetchRemove(entry.key)) |chain_kv| {
            var chain = chain_kv.value;
            for (chain.items.items) |ver| {
                if (ver.value.len > 0) self.allocator.free(@constCast(ver.value));
            }
            chain.items.deinit(self.allocator);
        }
        self.allocator.free(@constCast(entry.key));
        if (entry.value.len > 0 and !entry.tombstone) {
            self.allocator.free(@constCast(entry.value));
        }
        // Tombstones have value = "" (static), don't free
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn applyTest(kv: *KVProjection, e: *const Entry) router_mod.ApplyError!void {
    var out = Result.start(e);
    return kv.applyEntry(e, &out);
}

fn applyFor(kv: *KVProjection, e: *const Entry) router_mod.ApplyError!Result {
    var out = Result.start(e);
    try kv.applyEntry(e, &out);
    return out;
}

test "kv: an apply answers from what it did: the version set, whether a delete found the key, a refused incr" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();
    var buf: [256]u8 = undefined;
    const stamp: u64 = 1_000 * std.time.ns_per_s;

    var r = try applyFor(&kv, &putEntry(&buf, "k", "v", 1, stamp, 0));
    var rd = r.reader();
    try testing.expectEqual(@as(?u64, 1), rd.int(u64));
    r = try applyFor(&kv, &putEntry(&buf, "k", "w", 2, stamp, 0));
    rd = r.reader();
    try testing.expectEqual(@as(?u64, 2), rd.int(u64));

    // A delete says whether the key was there when it applied.
    r = try applyFor(&kv, &testEntry(&buf, .kv_delete, .{ .key = "k" }, 3, stamp));
    rd = r.reader();
    try testing.expectEqual(@as(?u8, 1), rd.int(u8));
    r = try applyFor(&kv, &testEntry(&buf, .kv_delete, .{ .key = "k" }, 4, stamp));
    rd = r.reader();
    try testing.expectEqual(@as(?u8, 0), rd.int(u8));
    // One whose TTL ran out by the delete's stamp wasn't there either.
    _ = try applyFor(&kv, &putEntry(&buf, "t", "v", 5, stamp, 1_000));
    r = try applyFor(&kv, &testEntry(&buf, .kv_delete, .{ .key = "t" }, 6, stamp + 2 * std.time.ns_per_s));
    rd = r.reader();
    try testing.expectEqual(@as(?u8, 0), rd.int(u8));

    // An incr that would overflow changes nothing and is refused.
    var max: [8]u8 = undefined;
    std.mem.writeInt(i64, &max, std.math.maxInt(i64), .little);
    _ = try applyFor(&kv, &putEntry(&buf, "c", &max, 7, stamp, 0));
    r = try applyFor(&kv, &testEntry(&buf, .kv_incr, .{ .key = "c", .delta = 1 }, 8, stamp));
    try testing.expectEqual(proto_status.bad_request, r.refused().?.status);
    try testing.expectEqualStrings(KVProjection.INCR_OVERFLOW, r.refused().?.refusal.message);
    try testing.expectEqual(std.math.maxInt(i64), std.mem.readInt(i64, kv.get("c").?.value[0..8], .little));
    // One on a value that isn't a counter is refused too.
    _ = try applyFor(&kv, &putEntry(&buf, "s", "text", 9, stamp, 0));
    r = try applyFor(&kv, &testEntry(&buf, .kv_incr, .{ .key = "s", .delta = 1 }, 10, stamp));
    try testing.expectEqualStrings(KVProjection.NOT_A_COUNTER, r.refused().?.refusal.message);
    // One that applies answers the key's version and value.
    r = try applyFor(&kv, &testEntry(&buf, .kv_incr, .{ .key = "n", .delta = 5 }, 11, stamp));
    rd = r.reader();
    try testing.expectEqual(@as(?u64, 1), rd.int(u64));
    try testing.expectEqual(@as(?i64, 5), rd.int(i64));
    // An expire of a key that's gone is refused as not found.
    r = try applyFor(&kv, &testEntry(&buf, .kv_expire, .{ .key = "gone", .ttl_ms = 10 }, 12, stamp));
    try testing.expectEqual(proto_status.not_found, r.refused().?.status);
}

const proto_status = @import("../protocol/proto.zig").StatusCode;

test "kv: basic put and get" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.put("key1", "value1", 1, 1, 1000, 0);
    try kv.put("key2", "value2", 2, 1, 2000, 0);

    const e1 = kv.get("key1").?;
    try testing.expectEqualSlices(u8, "value1", e1.value);
    try testing.expectEqual(@as(u64, 1), e1.lsn);

    const e2 = kv.get("key2").?;
    try testing.expectEqualSlices(u8, "value2", e2.value);

    try testing.expect(kv.get("nonexistent") == null);
    try testing.expectEqual(@as(usize, 2), kv.count());
}

test "kv: put overwrites" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.put("key1", "old", 1, 1, 1000, 0);
    try kv.put("key1", "new", 2, 1, 2000, 0);

    const e = kv.get("key1").?;
    try testing.expectEqualSlices(u8, "new", e.value);
    try testing.expectEqual(@as(u64, 2), e.lsn);
    try testing.expectEqual(@as(usize, 1), kv.count());
}

test "kv: delete creates tombstone" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.put("key1", "value1", 1, 1, 1000, 0);
    try kv.delete("key1", 2, 1, 2000);

    // get returns null for tombstone
    try testing.expect(kv.get("key1") == null);

    // getRaw still finds it
    const raw = kv.getRaw("key1").?;
    try testing.expect(raw.tombstone);

    try testing.expectEqual(@as(usize, 0), kv.count());
    try testing.expectEqual(@as(usize, 1), kv.totalEntries());
}

test "kv: delete nonexistent key creates tombstone" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.delete("ghost", 1, 1, 1000);

    try testing.expect(kv.get("ghost") == null);
    try testing.expectEqual(@as(usize, 1), kv.totalEntries());
}

test "kv: scan returns live entries" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.put("a", "1", 1, 1, 1000, 0);
    try kv.put("b", "2", 2, 1, 2000, 0);
    try kv.put("c", "3", 3, 1, 3000, 0);
    try kv.delete("b", 4, 1, 4000);

    var results: [10]ScanEntry = undefined;
    const n = kv.scan(&results);
    try testing.expectEqual(@as(usize, 2), n);
}

test "kv: scan with prefix" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.put("user:1", "alice", 1, 1, 1000, 0);
    try kv.put("user:2", "bob", 2, 1, 2000, 0);
    try kv.put("item:1", "sword", 3, 1, 3000, 0);

    var results: [10]ScanEntry = undefined;
    const n = kv.scanPrefix("user:", &results);
    try testing.expectEqual(@as(usize, 2), n);
}

test "kv: compact removes tombstones" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.put("a", "1", 1, 1, 1000, 0);
    try kv.put("b", "2", 2, 1, 2000, 0);
    try kv.delete("a", 3, 1, 3000);

    try testing.expectEqual(@as(usize, 2), kv.totalEntries());

    const purged = kv.compact();
    try testing.expectEqual(@as(usize, 1), purged);
    try testing.expectEqual(@as(usize, 1), kv.totalEntries());
}

test "kv: apply UAL entry for kv_put" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();
    var buf: [256]u8 = undefined;
    try applyTest(&kv, &putEntry(&buf, "mykey", "myval", 1, 1000, 0));
    const result = kv.get("mykey").?;
    try testing.expectEqualSlices(u8, "myval", result.value);
    try testing.expectEqual(@as(u64, 1), kv.applied_index);
}

test "kv: apply UAL entry for kv_delete" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();
    var buf: [256]u8 = undefined;
    try applyTest(&kv, &putEntry(&buf, "delme", "v", 1, 1000, 0));
    try applyTest(&kv, &testEntry(&buf, .kv_delete, .{ .key = "delme" }, 2, 2000));
    try testing.expect(kv.get("delme") == null);
    try testing.expectEqual(@as(u64, 2), kv.applied_index);
}

test "kv: projection handle integrates with router" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    var router = router_mod.ProjectionRouter.init();
    router.registerKV(kv.projectionHandle());

    var pb: [256]u8 = undefined;
    const entry = putEntry(&pb, "rkey", "rval", 1, 1000, 0);

    var out = Result.start(&entry);
    const result = try router.apply(&entry, &out);
    try testing.expectEqual(router_mod.ApplyResult.applied, result);

    // Verify it landed in the KV projection
    const kv_entry = kv.get("rkey").?;
    try testing.expectEqualSlices(u8, "rval", kv_entry.value);
}

test "kv: stats tracking" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.put("k", "v", 1, 1, 0, 0);
    _ = kv.get("k");
    _ = kv.get("missing");
    try kv.delete("k", 2, 1, 0);

    try testing.expectEqual(@as(u64, 1), kv.stats.puts);
    try testing.expectEqual(@as(u64, 2), kv.stats.gets);
    try testing.expectEqual(@as(u64, 1), kv.stats.deletes);
}

fn historyOf(kv: *KVProjection, key: []const u8, before: ?u64, out: []VersionEntry) usize {
    var it = kv.history(key, before);
    var n: usize = 0;
    while (it.next()) |v| : (n += 1) {
        if (n == out.len) break;
        out[n] = v;
    }
    return n;
}

test "kv: history is the current version, then the older ones newest first, from below a version when given" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.put("k", "v1", 1, 1, 100, 0);
    try kv.put("k", "v2", 2, 1, 200, 0);
    try kv.put("k", "v3", 3, 1, 300, 0);

    var hist: [10]VersionEntry = undefined;
    try testing.expectEqual(@as(usize, 3), historyOf(&kv, "k", null, &hist));
    for (hist[0..3], [_][]const u8{ "v3", "v2", "v1" }, [_]u64{ 3, 2, 1 }) |v, value, version| {
        try testing.expectEqualStrings(value, v.value);
        try testing.expectEqual(version, v.version);
    }
    try testing.expectEqual(@as(usize, 2), historyOf(&kv, "k", 3, &hist));
    try testing.expectEqual(@as(u64, 2), hist[0].version);
    try testing.expectEqual(@as(usize, 0), historyOf(&kv, "k", 1, &hist));
    try testing.expect(kv.history("k", 1).exists());
    try testing.expect(!kv.history("missing", null).exists());
}

test "kv: a delete keeps the history, the live value before it included" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.put("k", "v1", 1, 1, 100, 0);
    try kv.delete("k", 2, 1, 200);

    var hist: [10]VersionEntry = undefined;
    try testing.expectEqual(@as(usize, 2), historyOf(&kv, "k", null, &hist));
    try testing.expect(hist[0].tombstone);
    try testing.expectEqual(@as(u64, 2), hist[0].version);
    try testing.expectEqualStrings("v1", hist[1].value);
    try testing.expect(!hist[1].tombstone);
}

test "kv: history keeps at most HISTORY_MAX_VERSIONS older versions, oldest evicted first" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    const total = kv_ops.HISTORY_MAX_VERSIONS + 3;
    for (0..total) |i| {
        var buf: [8]u8 = undefined;
        try kv.put("k", std.fmt.bufPrint(&buf, "v{d}", .{i}) catch unreachable, @intCast(i + 1), 1, @intCast((i + 1) * 100), 0);
    }
    var hist: [kv_ops.MAX_HISTORY + 1]VersionEntry = undefined;
    try testing.expectEqual(@as(usize, kv_ops.MAX_HISTORY), historyOf(&kv, "k", null, &hist));
    try testing.expectEqual(@as(u64, total), hist[0].version);
    // The oldest kept: versions 1 and 2 are gone.
    try testing.expectEqual(@as(u64, total - kv_ops.HISTORY_MAX_VERSIONS), hist[kv_ops.MAX_HISTORY - 1].version);
    const counts = kv.namespaceCounts("default");
    try testing.expectEqual(@as(u64, 2), counts.history_evicted);
    var charged: u64 = 0;
    for (hist[1..kv_ops.MAX_HISTORY]) |v| charged += v.value.len + kv_ops.HISTORY_ENTRY_COST;
    try testing.expectEqual(charged, counts.history_bytes);
}

test "kv: history keeps at most HISTORY_MAX_BYTES, but always the newest older version" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    // 4 KiB values: 15 fit under 64 KiB with their 64-byte charges.
    const value: [4096]u8 = @splat('s');
    for (0..20) |i| try kv.put("k", &value, @intCast(i + 1), 1, 0, 0);
    var hist: [kv_ops.MAX_HISTORY]VersionEntry = undefined;
    const per = value.len + kv_ops.HISTORY_ENTRY_COST;
    const kept = kv_ops.HISTORY_MAX_BYTES / per;
    try testing.expectEqual(1 + kept, historyOf(&kv, "k", null, &hist));
    try testing.expectEqual(@as(u64, kept * per), kv.namespaceCounts("default").history_bytes);

    // One value larger than the whole budget evicts everything older, and
    // is itself kept once something overwrites it.
    const big = try testing.allocator.alloc(u8, kv_ops.HISTORY_MAX_BYTES + 1);
    defer testing.allocator.free(big);
    @memset(big, 'b');
    try kv.put("k", big, 21, 1, 0, 0);
    try kv.put("k", "after", 22, 1, 0, 0);
    try testing.expectEqual(@as(usize, 2), historyOf(&kv, "k", null, &hist));
    try testing.expectEqual(big.len, hist[1].value.len);
    try testing.expectEqual(@as(u64, big.len + kv_ops.HISTORY_ENTRY_COST), kv.namespaceCounts("default").history_bytes);
}

test "kv: a namespace's counts follow its keys: bytes, history, and removal frees both" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();

    try kv.put("ns\x00a", "123456", 1, 1, 0, 0);
    try testing.expectEqual(KVProjection.LiveKeys.Counts{ .live = 1, .bytes = 10 }, kv.namespaceCounts("ns"));
    try kv.put("ns\x00a", "x", 2, 1, 0, 0);
    try testing.expectEqual(KVProjection.LiveKeys.Counts{ .live = 1, .bytes = 5, .history_bytes = 6 + kv_ops.HISTORY_ENTRY_COST }, kv.namespaceCounts("ns"));
    try kv.delete("ns\x00a", 3, 1, 0);
    try testing.expectEqual(KVProjection.LiveKeys.Counts{ .live = 0, .bytes = 4, .history_bytes = 7 + 2 * kv_ops.HISTORY_ENTRY_COST }, kv.namespaceCounts("ns"));
    try kv.put("ns\x00b", "y", 4, 1, 0, 0);

    try testing.expectEqual(@as(usize, 1), kv.compact());
    try testing.expectEqual(KVProjection.LiveKeys.Counts{ .live = 1, .bytes = 5 }, kv.namespaceCounts("ns"));
    try testing.expectEqual(@as(usize, 1), kv.clearByPrefix("ns\x00"));
    try testing.expectEqual(KVProjection.LiveKeys.Counts{}, kv.namespaceCounts("ns"));
    try testing.expect(!kv.history("ns\x00a", null).exists());
}

test "kv: an allocation failure in put or delete changes nothing" {
    // Every allocation each op makes fails in turn; each time the key, its
    // history and its namespace's counts are as they were, and nothing leaks.
    const Case = struct {
        fn run(allocator: Allocator, op: enum { put_new, put_over, put_over_tombstone, delete_over, delete_new }) !void {
            var kv = KVProjection.init(allocator);
            defer kv.deinit();
            // A seeded key and namespace, made before the op under test fails.
            try kv.put("ns\x00k", "old", 1, 1, 0, 0);
            // A tombstone with no chain yet, so a put over it must reserve one.
            try kv.delete("ns\x00d", 1, 1, 0);
            const before = kv.namespaceCounts("ns");
            const result = switch (op) {
                .put_new => kv.put("ns\x00n", "new", 2, 1, 0, 0),
                .put_over => kv.put("ns\x00k", "new", 2, 1, 0, 0),
                .put_over_tombstone => kv.put("ns\x00d", "back", 3, 1, 0, 0),
                .delete_over => kv.delete("ns\x00k", 2, 1, 0),
                .delete_new => kv.delete("ns\x00n", 2, 1, 0),
            };
            result catch |err| {
                try testing.expectEqual(before, kv.namespaceCounts("ns"));
                try testing.expectEqual(@as(u64, 1), kv.getRaw("ns\x00k").?.version);
                try testing.expectEqualStrings("old", kv.getRaw("ns\x00k").?.value);
                try testing.expect(kv.getRaw("ns\x00n") == null);
                try testing.expect(kv.getRaw("ns\x00d").?.tombstone);
                try testing.expectEqual(@as(u64, 1), kv.getRaw("ns\x00d").?.version);
                try testing.expect(kv.version_chains.get("ns\x00d") == null);
                return err;
            };
        }
    };
    inline for (.{ .put_new, .put_over, .put_over_tombstone, .delete_over, .delete_new }) |op| {
        try testing.checkAllAllocationFailures(testing.allocator, Case.run, .{op});
    }
}

// ── Time: TTLs count from the entry's stamp ──────────────────────────────

/// A clock a test sets, for reads.
const TestReadClock = struct {
    ns: u64,

    fn read(ctx: ?*const anyopaque) u64 {
        const self: *const TestReadClock = @ptrCast(@alignCast(ctx.?));
        return self.ns;
    }
};

/// KV entry `entry_type` carrying `payload`, at `index`, stamped `stamp`.
fn testEntry(buf: []u8, comptime entry_type: entry_mod.EntryType, payload: kv_entries.Payload(@backingInt(entry_type)).Value, index: u64, stamp: u64) entry_mod.Entry {
    const bytes = kv_entries.Payload(@backingInt(entry_type)).encodeInto(buf, payload) catch unreachable;
    return entry_mod.buildEntry(entry_type, 0, 1, index, stamp, bytes);
}

/// A put of `value` at `key`, with a TTL unless it is 0.
fn putEntry(buf: []u8, key: []const u8, value: []const u8, index: u64, stamp: u64, ttl_ms: u64) entry_mod.Entry {
    return testEntry(buf, .kv_put, .{ .key = key, .value = value, .ttl_ms = if (ttl_ms == 0) null else ttl_ms }, index, stamp);
}

test "kv: a TTL runs out a TTL after the stamp of the entry that set it, on every replica alike, and reads judge it by the read clock" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();
    var clock: TestReadClock = .{ .ns = 0 };
    kv.read_clock = .{ .ctx = &clock, .now_ns = TestReadClock.read };
    const stamp: u64 = 1_000 * std.time.ns_per_s;
    var buf: [128]u8 = undefined;
    try applyTest(&kv, &putEntry(&buf, "k", "v", 1, stamp, 5_000));
    try testing.expectEqual(stamp + 5 * std.time.ns_per_s, kv.getRaw("k").?.expiry_ns);

    clock.ns = stamp + 5 * std.time.ns_per_s - 1;
    try testing.expect(kv.get("k") != null);
    var out: [4]ScanEntry = undefined;
    try testing.expectEqual(@as(usize, 1), kv.scan(&out));
    clock.ns += 1;
    try testing.expect(kv.get("k") == null);
    try testing.expectEqual(@as(usize, 0), kv.scan(&out));
    try testing.expectEqual(@as(usize, 0), kv.scanPrefix("k", &out));
    var names: [4][]const u8 = undefined;
    try testing.expectEqual(@as(usize, 0), kv.scanKeyNames("", &names));

    // An expire counts from its own entry's stamp; a TTL of 0 clears it.
    try applyTest(&kv, &putEntry(&buf, "t", "v", 2, stamp, 1_000));
    try applyTest(&kv, &testEntry(&buf, .kv_expire, .{ .key = "t", .ttl_ms = 60_000 }, 3, stamp + 500 * std.time.ns_per_ms));
    try testing.expectEqual(stamp + 60_500 * std.time.ns_per_ms, kv.getRaw("t").?.expiry_ns);
    try applyTest(&kv, &testEntry(&buf, .kv_expire, .{ .key = "t", .ttl_ms = 0 }, 4, stamp + std.time.ns_per_s));
    try testing.expectEqual(@as(u64, 0), kv.getRaw("t").?.expiry_ns);
}

test "kv: a key whose TTL ran out by an entry's stamp is gone to that entry: incr starts again from zero and touch finds nothing" {
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();
    const stamp: u64 = 1_000 * std.time.ns_per_s;
    var buf: [128]u8 = undefined;
    var counter: [8]u8 = undefined;
    std.mem.writeInt(i64, &counter, 7, .little);
    try applyTest(&kv, &putEntry(&buf, "c", &counter, 1, stamp, 1_000));
    // Before the TTL runs out it counts on; after, it starts again.
    try applyTest(&kv, &testEntry(&buf, .kv_incr, .{ .key = "c", .delta = 1 }, 2, stamp + 999 * std.time.ns_per_ms));
    try testing.expectEqual(@as(i64, 8), std.mem.readInt(i64, kv.getRaw("c").?.value[0..8], .little));
    try applyTest(&kv, &putEntry(&buf, "c", &counter, 3, stamp + 2 * std.time.ns_per_s, 1_000));
    try applyTest(&kv, &testEntry(&buf, .kv_incr, .{ .key = "c", .delta = 1 }, 4, stamp + 3 * std.time.ns_per_s));
    try testing.expectEqual(@as(i64, 1), std.mem.readInt(i64, kv.getRaw("c").?.value[0..8], .little));
    try testing.expectEqual(@as(u64, 0), kv.getRaw("c").?.expiry_ns);

    // An expire after the TTL ran out does not bring the key back.
    try applyTest(&kv, &putEntry(&buf, "t", "v", 5, stamp, 1_000));
    try applyTest(&kv, &testEntry(&buf, .kv_expire, .{ .key = "t", .ttl_ms = 60_000 }, 6, stamp + std.time.ns_per_s));
    try testing.expectEqual(stamp + std.time.ns_per_s, kv.getRaw("t").?.expiry_ns);
}

test "kv: a transaction's TTLs count from its batch's stamp" {
    const txn = @import("../kv/txn.zig");
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();
    const stamp: u64 = 1_000 * std.time.ns_per_s;
    var eb: [64]u8 = undefined;
    try applyTest(&kv, &putEntry(&eb, "t", "v", 1, stamp, 0));
    const ops = [_]txn.TxnOp{
        .{ .kind = .put, .key = @constCast("p"), .value = @constCast("v"), .ttl_ms = 2_000 },
        .{ .kind = .expire, .key = @constCast("t"), .value = @constCast(""), .ttl_ms = 30_000 },
    };
    const payload = try txn.encodeBatch(testing.allocator, &ops);
    defer testing.allocator.free(payload);
    const later = stamp + 10 * std.time.ns_per_s;
    try applyTest(&kv, &entry_mod.buildEntry(.kv_batch, 0, 1, 2, later, payload));
    try testing.expectEqual(later + 2 * std.time.ns_per_s, kv.getRaw("p").?.expiry_ns);
    try testing.expectEqual(later + 30 * std.time.ns_per_s, kv.getRaw("t").?.expiry_ns);
}

test "kv: a batch that doesn't decode whole, or breaks a kind's rule, applies nothing" {
    const txn = @import("../kv/txn.zig");
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();
    const ops = [_]txn.TxnOp{
        .{ .kind = .put, .key = @constCast("a"), .value = @constCast("1"), .ttl_ms = 0 },
        .{ .kind = .put, .key = @constCast("b"), .value = @constCast("2"), .ttl_ms = 0 },
    };
    const payload = try txn.encodeBatch(testing.allocator, &ops);
    defer testing.allocator.free(payload);
    // Cut inside the second op: the first must not apply on its own.
    for (1..payload.len) |n| {
        try testing.expectError(error.Halt, applyTest(&kv, &entry_mod.buildEntry(.kv_batch, 0, 1, 1, 0, payload[0..n])));
        try testing.expect(kv.get("a") == null);
    }
    // A delete carrying a value decodes, but breaks its kind's rule.
    var buf: [64]u8 = undefined;
    var w: schema.Writer = .{ .buf = &buf };
    try kv_entries.BatchOps.encodeSlice(&w, &.{
        .{ .kind = .put, .key = "a", .value = "1", .ttl_ms = null, .delta = null },
        .{ .kind = .delete, .key = "b", .value = "x", .ttl_ms = null, .delta = null },
    });
    try testing.expectError(error.Halt, applyTest(&kv, &entry_mod.buildEntry(.kv_batch, 0, 1, 1, 0, w.written())));
    try testing.expect(kv.get("a") == null);
    try testing.expectEqual(@as(u64, 0), kv.applied_index);
}

/// Live keys counted from scratch, per namespace, to check the kept count by.
fn recountLive(kv: *const KVProjection, namespace: []const u8) u64 {
    var n: u64 = 0;
    var it = kv.map.iterator();
    while (it.next()) |e| {
        if (!e.value_ptr.tombstone and std.mem.eql(u8, KVProjection.namespaceOf(e.key_ptr.*), namespace)) n += 1;
    }
    return n;
}

test "kv: live keys are counted per namespace as apply makes and removes them, and a replay counts the same" {
    const txn = @import("../kv/txn.zig");
    var kv = KVProjection.init(testing.allocator);
    defer kv.deinit();
    const stamp: u64 = 1_000 * std.time.ns_per_s;
    var bufs: [12][96]u8 = undefined;
    const batch_ops = [_]txn.TxnOp{
        .{ .kind = .put, .key = @constCast("orders\x00b1"), .value = @constCast("v"), .ttl_ms = 0 },
        .{ .kind = .put, .key = @constCast("orders\x00b2"), .value = @constCast("v"), .ttl_ms = 0 },
        .{ .kind = .delete, .key = @constCast("orders\x00b1"), .value = @constCast(""), .ttl_ms = 0 },
    };
    const batch = try txn.encodeBatch(testing.allocator, &batch_ops);
    defer testing.allocator.free(batch);
    // Each entry, with the counts after it: "orders", then the default namespace.
    const Step = struct { entry: Entry, orders: u64, default: u64 };
    const steps = [_]Step{
        .{ .entry = putEntry(&bufs[0], "orders\x00a", "1", 1, stamp, 0), .orders = 1, .default = 0 },
        .{ .entry = putEntry(&bufs[1], "orders\x00a", "2", 2, stamp, 0), .orders = 1, .default = 0 }, // overwrite
        .{ .entry = putEntry(&bufs[2], "plain", "1", 3, stamp, 0), .orders = 1, .default = 1 },
        .{ .entry = testEntry(&bufs[3], .kv_delete, .{ .key = "orders\x00missing" }, 4, stamp), .orders = 1, .default = 1 },
        .{ .entry = testEntry(&bufs[4], .kv_delete, .{ .key = "orders\x00a" }, 5, stamp), .orders = 0, .default = 1 },
        .{ .entry = testEntry(&bufs[5], .kv_delete, .{ .key = "orders\x00a" }, 6, stamp), .orders = 0, .default = 1 }, // again
        .{ .entry = putEntry(&bufs[6], "orders\x00a", "3", 7, stamp, 0), .orders = 1, .default = 1 }, // back from a tombstone
        .{ .entry = testEntry(&bufs[7], .kv_incr, .{ .key = "orders\x00n", .delta = 1 }, 8, stamp), .orders = 2, .default = 1 },
        .{ .entry = testEntry(&bufs[8], .kv_incr, .{ .key = "orders\x00n", .delta = 1 }, 9, stamp), .orders = 2, .default = 1 },
        .{ .entry = entry_mod.buildEntry(.kv_batch, 0, 1, 10, stamp, batch), .orders = 3, .default = 1 },
        // A TTL that runs out removes nothing: the key counts until deleted.
        .{ .entry = putEntry(&bufs[9], "plain", "t", 11, stamp, 1), .orders = 3, .default = 1 },
        .{ .entry = testEntry(&bufs[10], .kv_expire, .{ .key = "orders\x00a", .ttl_ms = 1 }, 12, stamp + std.time.ns_per_s), .orders = 3, .default = 1 },
    };
    for (steps) |step| {
        try applyTest(&kv, &step.entry);
        try testing.expectEqual(step.orders, kv.liveKeys("orders"));
        try testing.expectEqual(step.default, kv.liveKeys("default"));
        try testing.expectEqual(recountLive(&kv, "orders"), kv.liveKeys("orders"));
        try testing.expectEqual(recountLive(&kv, "default"), kv.liveKeys("default"));
    }
    try testing.expect(kv.live_keys_changed);

    // The same log applied again, as at boot or on a follower, counts the same.
    var replay = KVProjection.init(testing.allocator);
    defer replay.deinit();
    for (steps) |step| try applyTest(&replay, &step.entry);
    try testing.expectEqual(kv.liveKeys("orders"), replay.liveKeys("orders"));
    try testing.expectEqual(kv.liveKeys("default"), replay.liveKeys("default"));

    // Clearing a namespace takes its live keys out.
    _ = kv.clearByPrefix("orders\x00");
    try testing.expectEqual(@as(u64, 0), kv.liveKeys("orders"));
    try testing.expectEqual(@as(u64, 1), kv.liveKeys("default"));
}

test "kv: a put short of memory at any point leaves the count agreeing with the keys" {
    const run = struct {
        fn f(allocator: Allocator) !void {
            var kv = KVProjection.init(allocator);
            defer kv.deinit();
            const stamp: u64 = 1_000 * std.time.ns_per_s;
            defer {
                std.testing.expectEqual(recountLive(&kv, "ns"), kv.liveKeys("ns")) catch @panic("count disagrees with the keys");
            }
            try kv.put("ns\x00a", "1", 1, 1, stamp, 0);
            try kv.delete("ns\x00a", 2, 1, stamp);
            try kv.put("ns\x00a", "2", 3, 1, stamp, 0);
            try kv.put("ns\x00b", "1", 4, 1, stamp, 0);
        }
    }.f;
    try std.testing.checkAllAllocationFailures(testing.allocator, run, .{});
}
