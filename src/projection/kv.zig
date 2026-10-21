//! KV Projection — in-memory hash table with MVCC versioning.
//!
//! Maintains a hash map of key → KVEntry for O(1) point lookups.
//! Supports put, get, delete, scan, and TTL expiry.
//! Consumer group state is also stored here as regular KV entries.
//!
//! Bounded by a configurable memory limit. When the limit is approached,
//! old MVCC versions are pruned first.
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

/// Version chain length per key; nothing overrides it yet.
pub const DEFAULT_VERSION_CHAIN_LEN: usize = 64;

/// A historical version entry — stores previous value and metadata.
pub const VersionEntry = struct {
    value: []const u8,
    version: u64,
    lsn: u64,
    term: u64,
    timestamp_ns: u64,
    tombstone: bool,
};

pub const KVProjection = struct {
    allocator: Allocator,
    /// Main hash map: key → KVEntry.
    map: std.StringHashMap(KVEntry),
    /// Version chains: key → previous versions (newest first, bounded).
    version_chains: std.StringHashMap(std.ArrayListUnmanaged(VersionEntry)),
    /// Memory limit in bytes (0 = unlimited).
    memory_limit: usize,
    /// Approximate current memory usage.
    memory_used: usize,
    /// Last applied UAL index.
    applied_index: u64,
    /// Max versions per key in the hot chain. 0 = unlimited.
    max_version_chain_len: usize,

    /// Stats.
    stats: Stats,
    /// What "now" is when a read judges expiry.
    read_clock: ReadClock = .{},

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
        evicted: u64 = 0,
    };

    pub fn init(allocator: Allocator, memory_limit: usize) KVProjection {
        return .{
            .allocator = allocator,
            .map = std.StringHashMap(KVEntry).init(allocator),
            .version_chains = std.StringHashMap(std.ArrayListUnmanaged(VersionEntry)).init(allocator),
            .memory_limit = memory_limit,
            .memory_used = 0,
            .applied_index = 0,
            .max_version_chain_len = DEFAULT_VERSION_CHAIN_LEN,
            .stats = .{},
        };
    }

    pub fn deinit(self: *KVProjection) void {
        var it = self.map.iterator();
        while (it.next()) |kv| {
            self.freeEntry(kv.value_ptr);
        }
        self.map.deinit();

        // Free version chains
        var vc_it = self.version_chains.iterator();
        while (vc_it.next()) |entry| {
            for (entry.value_ptr.items) |ver| {
                if (ver.value.len > 0) self.allocator.free(@constCast(ver.value));
            }
            entry.value_ptr.deinit(self.allocator);
        }
        self.version_chains.deinit();
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

    /// Put a key-value pair, specifying the logical value type.
    pub fn putWithType(self: *KVProjection, key: []const u8, value: []const u8, lsn: u64, term: u64, timestamp_ns: u64, expiry_ns: u64, value_type: ValueType) !void {
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);

        if (self.map.getPtr(key)) |existing| {
            // Save current version to history before overwriting
            self.pushVersionChain(existing) catch {};

            // Key already in map — don't replace it (map references old key ptr).
            // Free the newly allocated key since we don't need it.
            self.allocator.free(owned_key);
            // Old value was moved to version chain (or freed if chain full)
            existing.value = owned_value;
            existing.lsn = lsn;
            existing.version += 1;
            existing.term = term;
            existing.timestamp_ns = timestamp_ns;
            existing.expiry_ns = expiry_ns;
            existing.tombstone = false;
            existing.value_type = value_type;
            self.memory_used += owned_value.len;
        } else {
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
            self.memory_used += owned_key.len + owned_value.len;
        }
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

    /// Delete a key (mark as tombstone).
    pub fn delete(self: *KVProjection, key: []const u8, lsn: u64, term: u64, timestamp_ns: u64) !void {
        if (self.map.getPtr(key)) |existing| {
            // Save current version to history before overwriting
            self.pushVersionChain(existing) catch {};

            // Old value was moved to version chain (or freed if chain full)
            existing.value = "";
            existing.lsn = lsn;
            existing.version += 1;
            existing.term = term;
            existing.timestamp_ns = timestamp_ns;
            existing.tombstone = true;
        } else {
            // Insert a tombstone for a key we never saw (possible after snapshot)
            const owned_key = try self.allocator.dupe(u8, key);
            const entry = KVEntry{
                .key = owned_key,
                .value = "",
                .lsn = lsn,
                .version = 1,
                .term = term,
                .timestamp_ns = timestamp_ns,
                .expiry_ns = 0,
                .tombstone = true,
            };
            try self.map.put(owned_key, entry);
            self.memory_used += owned_key.len;
        }
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

    /// Remove all entries whose key starts with `prefix`.
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

            for (to_remove_buf[0..remove_count]) |key| {
                if (self.map.fetchRemove(key)) |removed| {
                    self.memory_used -|= removed.value.key.len + removed.value.value.len;
                    self.allocator.free(@constCast(removed.value.key));
                    if (removed.value.value.len > 0) {
                        self.allocator.free(@constCast(removed.value.value));
                    }
                }
            }
            total_removed += remove_count;
        }

        return total_removed;
    }

    // ─── Version Chain (MVCC History) ──────────────────────────────────────

    /// Push the current entry's state onto the version chain.
    /// Evicts the oldest version if chain exceeds max_version_chain_len.
    fn pushVersionChain(self: *KVProjection, existing: *KVEntry) !void {
        // Duplicate value for the historical entry
        const hist_value = if (existing.value.len > 0 and !existing.tombstone)
            existing.value // transfer ownership — caller must NOT free
        else
            @as([]const u8, "");

        const hist = VersionEntry{
            .value = hist_value,
            .version = existing.version,
            .lsn = existing.lsn,
            .term = existing.term,
            .timestamp_ns = existing.timestamp_ns,
            .tombstone = existing.tombstone,
        };

        // Update memory accounting: value moved from current to chain
        // (no net change yet — will be freed if evicted below)

        const gop = try self.version_chains.getOrPut(existing.key);
        if (!gop.found_existing) {
            gop.value_ptr.* = .empty;
        }
        var chain = gop.value_ptr;

        // Evict oldest if at capacity (0 = unlimited)
        if (self.max_version_chain_len > 0 and chain.items.len >= self.max_version_chain_len) {
            const oldest = chain.orderedRemove(0);
            if (oldest.value.len > 0) {
                self.memory_used -|= oldest.value.len;
                self.allocator.free(@constCast(oldest.value));
            }
        }

        try chain.append(self.allocator, hist);
    }

    /// Get version history for a key (newest first).
    /// Returns current entry + previous versions from the chain.
    /// Caller does NOT own the returned data — references are borrowed.
    pub fn getHistory(self: *KVProjection, key: []const u8, out: []VersionEntry) usize {
        var n: usize = 0;

        // Current version first
        if (self.map.getPtr(key)) |entry| {
            if (n < out.len) {
                out[n] = .{
                    .value = entry.value,
                    .version = entry.version,
                    .lsn = entry.lsn,
                    .term = entry.term,
                    .timestamp_ns = entry.timestamp_ns,
                    .tombstone = entry.tombstone,
                };
                n += 1;
            }
        }

        // Historical versions (newest to oldest = reverse iteration)
        if (self.version_chains.getPtr(key)) |chain| {
            var i: usize = chain.items.len;
            while (i > 0 and n < out.len) {
                i -= 1;
                out[n] = chain.items[i];
                n += 1;
            }
        }

        return n;
    }

    /// Prune all version chains, freeing historical entries.
    /// Returns the number of historical versions freed.
    pub fn pruneVersionChains(self: *KVProjection) usize {
        var freed: usize = 0;
        var vc_it = self.version_chains.iterator();
        while (vc_it.next()) |entry| {
            for (entry.value_ptr.items) |ver| {
                if (ver.value.len > 0) {
                    self.memory_used -|= ver.value.len;
                    self.allocator.free(@constCast(ver.value));
                }
                freed += 1;
            }
            entry.value_ptr.deinit(self.allocator);
            entry.value_ptr.* = .empty;
        }
        return freed;
    }

    /// Remove all tombstones and expired entries. Returns number purged.
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

        for (to_remove[0..remove_count]) |key| {
            if (self.map.fetchRemove(key)) |removed| {
                self.memory_used -= removed.value.key.len + removed.value.value.len;
                self.allocator.free(@constCast(removed.value.key));
                if (removed.value.value.len > 0) {
                    self.allocator.free(@constCast(removed.value.value));
                }
            }
        }

        self.stats.evicted += @intCast(remove_count);
        return remove_count;
    }

    // ─── UAL Entry application (ProjectionRouter interface) ────────────────

    /// Apply a committed UAL entry to this projection. A payload that
    /// doesn't decode whole is a bug in the proposer: nothing of it is
    /// applied, and it is `InvalidPayload`, counted as an apply error. An
    /// incr that overflows or meets a non-counter, and an expire of a key
    /// that's gone, change nothing and are applied: every replica reaches
    /// the same state.
    pub fn applyEntry(self: *KVProjection, entry: *const Entry) !void {
        const entry_type: EntryType = @fromBackingInt(@intCast(entry.header.entry_type));
        const h = entry.header;
        var diag: schema.Diagnostic = .{};

        switch (entry_type) {
            .kv_put => {
                const p = kv_entries.Put.decodeAll(entry.payload, &diag) catch return error.InvalidPayload;
                try self.put(p.key, p.value, h.index, h.term, h.timestamp_ns, time_units.expiryAfter(h.timestamp_ns, p.ttl_ms orelse 0));
            },
            .kv_delete => {
                const p = kv_entries.Delete.decodeAll(entry.payload, &diag) catch return error.InvalidPayload;
                try self.delete(p.key, h.index, h.term, h.timestamp_ns);
            },
            .kv_incr => {
                const p = kv_entries.Incr.decodeAll(entry.payload, &diag) catch return error.InvalidPayload;
                try self.applyIncrNoop(p.key, p.delta, h);
            },
            .kv_expire => {
                const p = kv_entries.Expire.decodeAll(entry.payload, &diag) catch return error.InvalidPayload;
                self.applyExpireNoop(p.key, p.ttl_ms, h);
            },
            .kv_batch => {
                // Decoded and checked whole before any op applies, so a bad
                // batch changes nothing.
                const batch = kv_entries.Batch.decodeAll(entry.payload, &diag) catch return error.InvalidPayload;
                var check = batch.ops.iterator();
                while (check.next() catch return error.InvalidPayload) |op| {
                    if (kv_entries.batchOpRule(op) != null) return error.InvalidPayload;
                }
                var it = batch.ops.iterator();
                while (it.next() catch return error.InvalidPayload) |op| switch (op.kind) {
                    .put => try self.put(op.key, op.value.?, h.index, h.term, h.timestamp_ns, time_units.expiryAfter(h.timestamp_ns, op.ttl_ms orelse 0)),
                    .delete => try self.delete(op.key, h.index, h.term, h.timestamp_ns),
                    .incr => try self.applyIncrNoop(op.key, op.delta.?, h),
                    .expire => self.applyExpireNoop(op.key, op.ttl_ms.?, h),
                };
            },
            else => {},
        }

        self.applied_index = entry.header.index;
    }

    /// An incr as the log applies it: overflow and a non-counter change
    /// nothing (the proposer checked both; a concurrent write can still
    /// reach either first).
    fn applyIncrNoop(self: *KVProjection, key: []const u8, delta: i64, h: entry_mod.Header) !void {
        _ = self.applyIncr(key, delta, h.index, h.term, h.timestamp_ns) catch |err| switch (err) {
            error.Overflow, error.NotACounter => {},
            else => return err,
        };
    }

    /// An expire as the log applies it: a TTL of 0 clears it, and a key
    /// that's gone changes nothing.
    fn applyExpireNoop(self: *KVProjection, key: []const u8, ttl_ms: u64, h: entry_mod.Header) void {
        self.applyTouch(key, time_units.expiryAfter(h.timestamp_ns, ttl_ms), h.index, h.term, h.timestamp_ns) catch |err| switch (err) {
            error.NotFound => {},
        };
    }

    /// ProjectionVTable implementation for use with ProjectionRouter.
    pub fn projectionHandle(self: *KVProjection) router_mod.ProjectionHandle {
        return .{
            .ctx = @ptrCast(self),
            .vtable = .{
                .applyFn = vtableApply,
                .memoryUsageFn = vtableMemory,
            },
        };
    }

    fn vtableApply(ctx: *anyopaque, entry: *const Entry) router_mod.ApplyError!void {
        const self: *KVProjection = @ptrCast(@alignCast(ctx));
        self.applyEntry(entry) catch return error.InvalidPayload;
    }

    fn vtableMemory(ctx: *anyopaque) usize {
        const self: *KVProjection = @ptrCast(@alignCast(ctx));
        return self.memoryUsage();
    }

    // ─── Memory ────────────────────────────────────────────────────────────

    pub fn memoryUsage(self: *const KVProjection) usize {
        // Approximate: tracked key+value bytes + per-entry overhead
        return self.memory_used + self.map.count() * @sizeOf(KVEntry);
    }

    // ─── Internal ──────────────────────────────────────────────────────────

    fn freeEntry(self: *KVProjection, entry: *KVEntry) void {
        // Free version chain for this key
        if (self.version_chains.fetchRemove(entry.key)) |chain_kv| {
            var chain = chain_kv.value;
            for (chain.items) |ver| {
                if (ver.value.len > 0) self.allocator.free(@constCast(ver.value));
            }
            chain.deinit(self.allocator);
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

test "kv: basic put and get" {
    var kv = KVProjection.init(testing.allocator, 0);
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
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("key1", "old", 1, 1, 1000, 0);
    try kv.put("key1", "new", 2, 1, 2000, 0);

    const e = kv.get("key1").?;
    try testing.expectEqualSlices(u8, "new", e.value);
    try testing.expectEqual(@as(u64, 2), e.lsn);
    try testing.expectEqual(@as(usize, 1), kv.count());
}

test "kv: delete creates tombstone" {
    var kv = KVProjection.init(testing.allocator, 0);
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
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.delete("ghost", 1, 1, 1000);

    try testing.expect(kv.get("ghost") == null);
    try testing.expectEqual(@as(usize, 1), kv.totalEntries());
}

test "kv: scan returns live entries" {
    var kv = KVProjection.init(testing.allocator, 0);
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
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("user:1", "alice", 1, 1, 1000, 0);
    try kv.put("user:2", "bob", 2, 1, 2000, 0);
    try kv.put("item:1", "sword", 3, 1, 3000, 0);

    var results: [10]ScanEntry = undefined;
    const n = kv.scanPrefix("user:", &results);
    try testing.expectEqual(@as(usize, 2), n);
}

test "kv: compact removes tombstones" {
    var kv = KVProjection.init(testing.allocator, 0);
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
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();
    var buf: [256]u8 = undefined;
    try kv.applyEntry(&putEntry(&buf, "mykey", "myval", 1, 1000, 0));
    const result = kv.get("mykey").?;
    try testing.expectEqualSlices(u8, "myval", result.value);
    try testing.expectEqual(@as(u64, 1), kv.applied_index);
}

test "kv: apply UAL entry for kv_delete" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();
    var buf: [256]u8 = undefined;
    try kv.applyEntry(&putEntry(&buf, "delme", "v", 1, 1000, 0));
    try kv.applyEntry(&testEntry(&buf, .kv_delete, .{ .key = "delme" }, 2, 2000));
    try testing.expect(kv.get("delme") == null);
    try testing.expectEqual(@as(u64, 2), kv.applied_index);
}

test "kv: projection handle integrates with router" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    var router = router_mod.ProjectionRouter.init();
    router.registerKV(kv.projectionHandle());

    var pb: [256]u8 = undefined;
    const entry = putEntry(&pb, "rkey", "rval", 1, 1000, 0);

    const result = router.apply(&entry);
    try testing.expectEqual(router_mod.ApplyResult.applied, result);

    // Verify it landed in the KV projection
    const kv_entry = kv.get("rkey").?;
    try testing.expectEqualSlices(u8, "rval", kv_entry.value);
}

test "kv: stats tracking" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("k", "v", 1, 1, 0, 0);
    _ = kv.get("k");
    _ = kv.get("missing");
    try kv.delete("k", 2, 1, 0);

    try testing.expectEqual(@as(u64, 1), kv.stats.puts);
    try testing.expectEqual(@as(u64, 2), kv.stats.gets);
    try testing.expectEqual(@as(u64, 1), kv.stats.deletes);
}

test "kv: memory tracking" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("abc", "123456", 1, 1, 0, 0);
    // key(3) + value(6) = 9 bytes tracked
    try testing.expectEqual(@as(usize, 9), kv.memory_used);

    try kv.put("abc", "x", 2, 1, 0, 0);
    // key(3) + new_value(1) + chain_value(6) = 10 bytes (old value retained in version chain)
    try testing.expectEqual(@as(usize, 10), kv.memory_used);

    // After pruning version chains, only current entry remains
    _ = kv.pruneVersionChains();
    try testing.expectEqual(@as(usize, 4), kv.memory_used);
}

test "kv: version chain basic" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("k", "v1", 1, 1, 100, 0);
    try kv.put("k", "v2", 2, 1, 200, 0);
    try kv.put("k", "v3", 3, 1, 300, 0);

    var hist: [10]VersionEntry = undefined;
    const n = kv.getHistory("k", &hist);
    try testing.expectEqual(@as(usize, 3), n);

    // Current version first
    try testing.expectEqualSlices(u8, "v3", hist[0].value);
    try testing.expectEqual(@as(u64, 3), hist[0].version);

    // Then historical (newest to oldest)
    try testing.expectEqualSlices(u8, "v2", hist[1].value);
    try testing.expectEqual(@as(u64, 2), hist[1].version);
    try testing.expectEqualSlices(u8, "v1", hist[2].value);
    try testing.expectEqual(@as(u64, 1), hist[2].version);
}

test "kv: version chain preserves history on delete" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("k", "v1", 1, 1, 100, 0);
    try kv.delete("k", 2, 1, 200);

    var hist: [10]VersionEntry = undefined;
    const n = kv.getHistory("k", &hist);
    try testing.expectEqual(@as(usize, 2), n);

    // Current is tombstone
    try testing.expect(hist[0].tombstone);
    try testing.expectEqual(@as(u64, 2), hist[0].version);

    // Previous was the live value
    try testing.expectEqualSlices(u8, "v1", hist[1].value);
    try testing.expect(!hist[1].tombstone);
}

test "kv: version chain bounded at max length" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    // Write DEFAULT_VERSION_CHAIN_LEN + 2 versions (current + chain)
    const total = DEFAULT_VERSION_CHAIN_LEN + 2;
    for (0..total) |i| {
        var buf: [8]u8 = undefined;
        const val = std.fmt.bufPrint(&buf, "v{d}", .{i}) catch unreachable;
        try kv.put("k", val, @intCast(i + 1), 1, @intCast((i + 1) * 100), 0);
    }

    var hist: [DEFAULT_VERSION_CHAIN_LEN + 1]VersionEntry = undefined;
    const n = kv.getHistory("k", &hist);
    // Current + DEFAULT_VERSION_CHAIN_LEN historical
    try testing.expectEqual(DEFAULT_VERSION_CHAIN_LEN + 1, n);

    // Oldest versions should have been evicted
    try testing.expectEqual(@as(u64, total), hist[0].version); // current
    try testing.expectEqual(@as(u64, total - 1), hist[1].version); // most recent historical
}

test "kv: version chain non-existent key returns 0" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    var hist: [10]VersionEntry = undefined;
    const n = kv.getHistory("missing", &hist);
    try testing.expectEqual(@as(usize, 0), n);
}

test "kv: prune version chains" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("a", "a1", 1, 1, 0, 0);
    try kv.put("a", "a2", 2, 1, 0, 0);
    try kv.put("b", "b1", 3, 1, 0, 0);
    try kv.put("b", "b2", 4, 1, 0, 0);

    const freed = kv.pruneVersionChains();
    try testing.expectEqual(@as(usize, 2), freed); // one old version per key

    // History should now only have current versions
    var hist: [10]VersionEntry = undefined;
    try testing.expectEqual(@as(usize, 1), kv.getHistory("a", &hist));
    try testing.expectEqual(@as(usize, 1), kv.getHistory("b", &hist));
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
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();
    var clock: TestReadClock = .{ .ns = 0 };
    kv.read_clock = .{ .ctx = &clock, .now_ns = TestReadClock.read };
    const stamp: u64 = 1_000 * std.time.ns_per_s;
    var buf: [128]u8 = undefined;
    try kv.applyEntry(&putEntry(&buf, "k", "v", 1, stamp, 5_000));
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
    try kv.applyEntry(&putEntry(&buf, "t", "v", 2, stamp, 1_000));
    try kv.applyEntry(&testEntry(&buf, .kv_expire, .{ .key = "t", .ttl_ms = 60_000 }, 3, stamp + 500 * std.time.ns_per_ms));
    try testing.expectEqual(stamp + 60_500 * std.time.ns_per_ms, kv.getRaw("t").?.expiry_ns);
    try kv.applyEntry(&testEntry(&buf, .kv_expire, .{ .key = "t", .ttl_ms = 0 }, 4, stamp + std.time.ns_per_s));
    try testing.expectEqual(@as(u64, 0), kv.getRaw("t").?.expiry_ns);
}

test "kv: a key whose TTL ran out by an entry's stamp is gone to that entry: incr starts again from zero and touch finds nothing" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();
    const stamp: u64 = 1_000 * std.time.ns_per_s;
    var buf: [128]u8 = undefined;
    var counter: [8]u8 = undefined;
    std.mem.writeInt(i64, &counter, 7, .little);
    try kv.applyEntry(&putEntry(&buf, "c", &counter, 1, stamp, 1_000));
    // Before the TTL runs out it counts on; after, it starts again.
    try kv.applyEntry(&testEntry(&buf, .kv_incr, .{ .key = "c", .delta = 1 }, 2, stamp + 999 * std.time.ns_per_ms));
    try testing.expectEqual(@as(i64, 8), std.mem.readInt(i64, kv.getRaw("c").?.value[0..8], .little));
    try kv.applyEntry(&putEntry(&buf, "c", &counter, 3, stamp + 2 * std.time.ns_per_s, 1_000));
    try kv.applyEntry(&testEntry(&buf, .kv_incr, .{ .key = "c", .delta = 1 }, 4, stamp + 3 * std.time.ns_per_s));
    try testing.expectEqual(@as(i64, 1), std.mem.readInt(i64, kv.getRaw("c").?.value[0..8], .little));
    try testing.expectEqual(@as(u64, 0), kv.getRaw("c").?.expiry_ns);

    // An expire after the TTL ran out does not bring the key back.
    try kv.applyEntry(&putEntry(&buf, "t", "v", 5, stamp, 1_000));
    try kv.applyEntry(&testEntry(&buf, .kv_expire, .{ .key = "t", .ttl_ms = 60_000 }, 6, stamp + std.time.ns_per_s));
    try testing.expectEqual(stamp + std.time.ns_per_s, kv.getRaw("t").?.expiry_ns);
}

test "kv: a transaction's TTLs count from its batch's stamp" {
    const txn = @import("../kv/txn.zig");
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();
    const stamp: u64 = 1_000 * std.time.ns_per_s;
    var eb: [64]u8 = undefined;
    try kv.applyEntry(&putEntry(&eb, "t", "v", 1, stamp, 0));
    const ops = [_]txn.TxnOp{
        .{ .kind = .put, .key = @constCast("p"), .value = @constCast("v"), .ttl_ms = 2_000 },
        .{ .kind = .expire, .key = @constCast("t"), .value = @constCast(""), .ttl_ms = 30_000 },
    };
    const payload = try txn.encodeBatch(testing.allocator, &ops);
    defer testing.allocator.free(payload);
    const later = stamp + 10 * std.time.ns_per_s;
    try kv.applyEntry(&entry_mod.buildEntry(.kv_batch, 0, 1, 2, later, payload));
    try testing.expectEqual(later + 2 * std.time.ns_per_s, kv.getRaw("p").?.expiry_ns);
    try testing.expectEqual(later + 30 * std.time.ns_per_s, kv.getRaw("t").?.expiry_ns);
}

test "kv: a batch that doesn't decode whole, or breaks a kind's rule, applies nothing" {
    const txn = @import("../kv/txn.zig");
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();
    const ops = [_]txn.TxnOp{
        .{ .kind = .put, .key = @constCast("a"), .value = @constCast("1"), .ttl_ms = 0 },
        .{ .kind = .put, .key = @constCast("b"), .value = @constCast("2"), .ttl_ms = 0 },
    };
    const payload = try txn.encodeBatch(testing.allocator, &ops);
    defer testing.allocator.free(payload);
    // Cut inside the second op: the first must not apply on its own.
    for (1..payload.len) |n| {
        try testing.expectError(error.InvalidPayload, kv.applyEntry(&entry_mod.buildEntry(.kv_batch, 0, 1, 1, 0, payload[0..n])));
        try testing.expect(kv.get("a") == null);
    }
    // A delete carrying a value decodes, but breaks its kind's rule.
    var buf: [64]u8 = undefined;
    var w: schema.Writer = .{ .buf = &buf };
    try kv_entries.BatchOps.encodeSlice(&w, &.{
        .{ .kind = .put, .key = "a", .value = "1", .ttl_ms = null, .delta = null },
        .{ .kind = .delete, .key = "b", .value = "x", .ttl_ms = null, .delta = null },
    });
    try testing.expectError(error.InvalidPayload, kv.applyEntry(&entry_mod.buildEntry(.kv_batch, 0, 1, 1, 0, w.written())));
    try testing.expect(kv.get("a") == null);
    try testing.expectEqual(@as(u64, 0), kv.applied_index);
}
