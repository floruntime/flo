//! KV Projection — in-memory hash table with MVCC versioning.
//!
//! Maintains a hash map of key → KVEntry for O(1) point lookups.
//! Supports put, get, delete, scan, and TTL expiry.
//! Consumer group state is also stored here as regular KV entries.
//!
//! Bounded by a configurable memory limit. When the limit is approached,
//! old MVCC versions are pruned first. The projection is snapshot-able
//! for Raft snapshot transfers.
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

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const CommandPayload = entry_mod.CommandPayload;

// ═══════════════════════════════════════════════════════════════════════════════
// KV Entry — stored in the hash map
// ═══════════════════════════════════════════════════════════════════════════════

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

pub const KVProjection = struct {
    allocator: Allocator,
    /// Main hash map: key → KVEntry.
    map: std.StringHashMap(KVEntry),
    /// Memory limit in bytes (0 = unlimited).
    memory_limit: usize,
    /// Approximate current memory usage.
    memory_used: usize,
    /// Last applied UAL index.
    applied_index: u64,

    /// Stats.
    stats: Stats,

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
            .memory_limit = memory_limit,
            .memory_used = 0,
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
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);

        if (self.map.getPtr(key)) |existing| {
            // Key already in map — don't replace it (map references old key ptr).
            // Free the newly allocated key since we don't need it.
            self.allocator.free(owned_key);
            // Free old value
            self.memory_used -= existing.value.len;
            if (existing.value.len > 0 and !existing.tombstone) {
                self.allocator.free(@constCast(existing.value));
            }
            existing.value = owned_value;
            existing.lsn = lsn;
            existing.version += 1;
            existing.term = term;
            existing.timestamp_ns = timestamp_ns;
            existing.expiry_ns = expiry_ns;
            existing.tombstone = false;
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
        if (entry.expiry_ns > 0 and entry.expiry_ns <= std.time.nanoTimestamp()) {
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
            // Free the value, keep the key for scan-skip
            self.memory_used -= existing.value.len;
            self.allocator.free(@constCast(existing.value));
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

    /// Scan all live (non-tombstone) entries. Results are unsorted.
    /// Caller provides a bounded output buffer.
    pub fn scan(self: *KVProjection, out: []ScanEntry) usize {
        self.stats.scans += 1;
        const now = std.time.nanoTimestamp();
        var count_written: usize = 0;
        var it = self.map.iterator();
        while (it.next()) |kv| {
            if (count_written >= out.len) break;
            const entry = kv.value_ptr;
            if (entry.tombstone) continue;
            if (entry.expiry_ns > 0 and entry.expiry_ns <= now) continue; // expired
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
        const now = std.time.nanoTimestamp();
        var count_written: usize = 0;
        var it = self.map.iterator();
        while (it.next()) |kv| {
            if (count_written >= out.len) break;
            const entry = kv.value_ptr;
            if (entry.tombstone) continue;
            if (entry.expiry_ns > 0 and entry.expiry_ns <= now) continue; // expired
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
        const now = std.time.nanoTimestamp();
        var n_found: usize = 0;
        var it = self.map.iterator();
        while (it.next()) |kv| {
            if (n_found >= out.len) break;
            const entry = kv.value_ptr;
            if (entry.tombstone) continue;
            if (entry.expiry_ns > 0 and entry.expiry_ns <= now) continue;
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

    /// Apply a committed UAL entry to this projection.
    pub fn applyEntry(self: *KVProjection, entry: *const Entry) !void {
        const entry_type: EntryType = @enumFromInt(entry.header.entry_type);

        switch (entry_type) {
            .kv_put, .cg_commit, .cg_create => {
                const cmd = CommandPayload.deserialize(entry.payload) orelse
                    return error.InvalidPayload;
                try self.put(
                    cmd.key,
                    cmd.value,
                    entry.header.index,
                    entry.header.term,
                    entry.header.timestamp_ns,
                    extractExpiry(entry, &cmd),
                );
            },
            .kv_delete, .cg_delete => {
                const cmd = CommandPayload.deserialize(entry.payload) orelse
                    return error.InvalidPayload;
                try self.delete(
                    cmd.key,
                    entry.header.index,
                    entry.header.term,
                    entry.header.timestamp_ns,
                );
            },
            .kv_batch => {
                // Batch entries contain multiple ops — for now, treat as single put
                const cmd = CommandPayload.deserialize(entry.payload) orelse
                    return error.InvalidPayload;
                try self.put(
                    cmd.key,
                    cmd.value,
                    entry.header.index,
                    entry.header.term,
                    entry.header.timestamp_ns,
                    extractExpiry(entry, &cmd),
                );
            },
            else => {},
        }

        self.applied_index = entry.header.index;
    }

    /// Extract expiry_ns from a UAL entry if the HAS_TTL flag is set.
    /// TTL is encoded as 8 bytes of u64 LE appended after the CommandPayload data.
    fn extractExpiry(entry: *const Entry, cmd: *const CommandPayload) u64 {
        const Flags = entry_mod.Flags;
        if (entry.header.flags & Flags.HAS_TTL == 0) return 0;

        // TTL bytes start after command prefix (ns_hash:4 + key_len:2 + val_len:4 = 10)
        // plus key + value data
        const COMMAND_PREFIX_SIZE = entry_mod.COMMAND_PREFIX_SIZE;
        const ttl_offset = COMMAND_PREFIX_SIZE + cmd.key_length + cmd.value_length;
        if (entry.payload.len >= ttl_offset + 8) {
            return std.mem.readInt(u64, entry.payload[ttl_offset..][0..8], .little);
        }
        return 0;
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

    // ─── Snapshot Serialization ────────────────────────────────────────────

    /// Serialize the full KV projection state to a byte buffer.
    /// Format: [entry_count: u64] then per entry:
    ///   [key_len: u32][value_len: u32][lsn: u64][version: u64][term: u64]
    ///   [timestamp_ns: u64][expiry_ns: u64][flags: u8]
    ///   [key bytes][value bytes]
    /// Caller owns the returned slice.
    pub fn serialize(self: *KVProjection, allocator: Allocator) ![]u8 {
        // Calculate total size
        const entry_count = self.map.count();
        var total_size: usize = 8; // entry_count: u64
        var it = self.map.iterator();
        while (it.next()) |kv| {
            const entry = kv.value_ptr;
            // key_len(4) + value_len(4) + lsn(8) + version(8) + term(8) + timestamp_ns(8) + expiry_ns(8) + flags(1)
            total_size += 49 + entry.key.len + entry.value.len;
        }

        const buf = try allocator.alloc(u8, total_size);
        errdefer allocator.free(buf);

        var offset: usize = 0;

        // Entry count
        std.mem.writeInt(u64, buf[offset..][0..8], @intCast(entry_count), .little);
        offset += 8;

        // Entries
        it = self.map.iterator();
        while (it.next()) |kv| {
            const entry = kv.value_ptr;

            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(entry.key.len), .little);
            offset += 4;
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(entry.value.len), .little);
            offset += 4;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.lsn, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.version, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.term, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.timestamp_ns, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.expiry_ns, .little);
            offset += 8;
            buf[offset] = if (entry.tombstone) 1 else 0;
            offset += 1;

            @memcpy(buf[offset..][0..entry.key.len], entry.key);
            offset += entry.key.len;
            if (entry.value.len > 0) {
                @memcpy(buf[offset..][0..entry.value.len], entry.value);
            }
            offset += entry.value.len;
        }

        return buf;
    }

    /// Restore KV projection state from serialized bytes.
    /// Clears all existing state before restoring.
    pub fn deserialize(self: *KVProjection, data: []const u8) !void {
        // Clear existing state
        var old_it = self.map.iterator();
        while (old_it.next()) |kv| {
            self.freeEntry(kv.value_ptr);
        }
        self.map.clearAndFree();
        self.memory_used = 0;

        if (data.len < 8) return;

        var offset: usize = 0;
        const entry_count = std.mem.readInt(u64, data[offset..][0..8], .little);
        offset += 8;

        var i: u64 = 0;
        while (i < entry_count) : (i += 1) {
            if (offset + 49 > data.len) return error.InvalidPayload;

            const key_len = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;
            const value_len = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;
            const lsn = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const version = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const term = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const timestamp_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const expiry_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const tombstone = data[offset] != 0;
            offset += 1;

            if (offset + key_len + value_len > data.len) return error.InvalidPayload;

            const key = data[offset..][0..key_len];
            offset += key_len;
            const value = data[offset..][0..value_len];
            offset += value_len;

            // Restore into hash map
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);

            const owned_value = if (value_len > 0 and !tombstone)
                try self.allocator.dupe(u8, value)
            else
                "";
            errdefer if (owned_value.len > 0) self.allocator.free(@constCast(owned_value));

            try self.map.put(owned_key, .{
                .key = owned_key,
                .value = owned_value,
                .lsn = lsn,
                .version = version,
                .term = term,
                .timestamp_ns = timestamp_ns,
                .expiry_ns = expiry_ns,
                .tombstone = tombstone,
            });
            self.memory_used += owned_key.len + owned_value.len;
        }
    }

    // ─── Internal ──────────────────────────────────────────────────────────

    fn freeEntry(self: *KVProjection, entry: *KVEntry) void {
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

    // Build a command payload: ns_hash(4) + key_len(2) + val_len(4) + key + value
    var payload_buf: [256]u8 = undefined;
    const cmd = entry_mod.CommandPayload{
        .namespace_hash = 0,
        .key_length = 5,
        .value_length = 5,
        .key = "mykey",
        .value = "myval",
    };
    const plen = cmd.serialize(&payload_buf) orelse unreachable;

    const entry = entry_mod.buildEntry(.kv_put, 0, 1, 1, 1000, payload_buf[0..plen]);

    try kv.applyEntry(&entry);
    const result = kv.get("mykey").?;
    try testing.expectEqualSlices(u8, "myval", result.value);
    try testing.expectEqual(@as(u64, 1), kv.applied_index);
}

test "kv: apply UAL entry for kv_delete" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    // First put
    var pb: [256]u8 = undefined;
    const put_cmd = entry_mod.CommandPayload{ .namespace_hash = 0, .key_length = 5, .value_length = 1, .key = "delme", .value = "v" };
    const put_len = put_cmd.serialize(&pb) orelse unreachable;
    try kv.applyEntry(&entry_mod.buildEntry(.kv_put, 0, 1, 1, 1000, pb[0..put_len]));

    // Then delete
    var db: [256]u8 = undefined;
    const del_cmd = entry_mod.CommandPayload{ .namespace_hash = 0, .key_length = 5, .value_length = 0, .key = "delme", .value = "" };
    const del_len = del_cmd.serialize(&db) orelse unreachable;
    try kv.applyEntry(&entry_mod.buildEntry(.kv_delete, 0, 1, 2, 2000, db[0..del_len]));

    try testing.expect(kv.get("delme") == null);
    try testing.expectEqual(@as(u64, 2), kv.applied_index);
}

test "kv: projection handle integrates with router" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    var router = router_mod.ProjectionRouter.init();
    router.registerKV(kv.projectionHandle());

    // Build a UAL entry with command payload
    var pb: [256]u8 = undefined;
    const cmd = entry_mod.CommandPayload{ .namespace_hash = 0, .key_length = 4, .value_length = 4, .key = "rkey", .value = "rval" };
    const plen = cmd.serialize(&pb) orelse unreachable;
    const entry = entry_mod.buildEntry(.kv_put, 0, 1, 1, 1000, pb[0..plen]);

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
    // Updated: key(3) + value(1) = 4 bytes
    try testing.expectEqual(@as(usize, 4), kv.memory_used);
}

test "kv: serialize/deserialize round-trip" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("key1", "value1", 1, 1, 1000, 0);
    try kv.put("key2", "value2", 2, 1, 2000, 5000);
    try kv.put("key3", "v3", 3, 2, 3000, 0);

    // Serialize
    const data = try kv.serialize(testing.allocator);
    defer testing.allocator.free(data);

    // Deserialize into a fresh projection
    var kv2 = KVProjection.init(testing.allocator, 0);
    defer kv2.deinit();

    try kv2.deserialize(data);

    // Verify all entries restored
    try testing.expectEqual(@as(usize, 3), kv2.count());

    const e1 = kv2.getRaw("key1").?;
    try testing.expectEqualStrings("value1", e1.value);
    try testing.expectEqual(@as(u64, 1), e1.lsn);
    try testing.expectEqual(@as(u64, 1), e1.term);
    try testing.expectEqual(@as(u64, 1000), e1.timestamp_ns);
    try testing.expectEqual(@as(u64, 0), e1.expiry_ns);

    const e2 = kv2.getRaw("key2").?;
    try testing.expectEqualStrings("value2", e2.value);
    try testing.expectEqual(@as(u64, 5000), e2.expiry_ns);

    const e3 = kv2.getRaw("key3").?;
    try testing.expectEqualStrings("v3", e3.value);
    try testing.expectEqual(@as(u64, 2), e3.term);
}

test "kv: serialize empty projection" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    const data = try kv.serialize(testing.allocator);
    defer testing.allocator.free(data);

    var kv2 = KVProjection.init(testing.allocator, 0);
    defer kv2.deinit();

    try kv2.deserialize(data);
    try testing.expectEqual(@as(usize, 0), kv2.count());
}
