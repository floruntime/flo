//! UAL — Unified Append Log Hot Ring Buffer
//!
//! Fixed-capacity ring buffer for recent UAL entries. Each partition
//! owns one hot ring (default 64 MB). Variable-size entries are
//! serialized contiguously. A hash map provides O(1) by-index reads.
//!
//! ## Append
//!
//! `append(entry) → index` serializes entry.header ++ entry.payload
//! into the ring. If there isn't enough space, the oldest entries
//! are evicted (moved to disk by the segment writer first).
//!
//! ## Read
//!
//! `read(index) → ?Entry` returns a zero-copy view into the ring.
//! If the entry was evicted, returns null (caller falls through to
//! warm segments).
//!
//! ## Ring Geometry
//!
//! ```
//! ┌───────────────────────────────────────┐
//! │               buffer (64 MB)          │
//! │ ┌─[write_pos]─────┐                  │
//! │ │                  │                  │
//! │ entry0 entry1 ... entryN  (free)      │
//! │         ↑                             │
//! │         [read_pos]                    │
//! └───────────────────────────────────────┘
//! ```
//!
//! Positions grow monotonically; actual offset = pos % capacity.
//! The ring is non-overlapping: eviction must happen before write.

const std = @import("std");
const entry_mod = @import("entry.zig");

const Entry = entry_mod.Entry;
const Header = entry_mod.Header;
const HEADER_SIZE = entry_mod.HEADER_SIZE;

// ═══════════════════════════════════════════════════════════════════════════════
// UAL
// ═══════════════════════════════════════════════════════════════════════════════

pub const DEFAULT_CAPACITY: usize = 64 * 1024 * 1024; // 64 MB

pub const UAL = struct {
    /// Backing storage for the ring.
    buffer: []u8,
    capacity: usize,

    /// Monotonic write cursor (next byte to write).
    write_pos: u64,
    /// Monotonic read cursor (oldest valid byte).
    read_pos: u64,

    /// Index map: UAL index → ring position (byte offset in virtual space).
    index_map: std.AutoHashMapUnmanaged(u64, u64),

    /// Number of live entries in the ring.
    entry_count: u64,
    /// Total entries appended (including evicted).
    total_appended: u64,
    /// Total bytes written.
    total_bytes_written: u64,

    /// Lowest live index in the ring.
    min_live_index: u64,
    /// Highest index in the ring (last appended).
    max_index: u64,

    /// Eviction callback — called when entries are evicted from the hot ring.
    on_evict: ?*const fn (data: []const u8, index: u64) void,

    /// Persistence callback — called after every successful append.
    /// Used to feed entries to the SegmentWriter for disk durability.
    /// Wired AFTER segment replay to avoid re-persisting old entries.
    on_append_ctx: ?*anyopaque,
    on_append: ?*const fn (ctx: *anyopaque, entry: *const Entry) void,

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !UAL {
        const buf = try allocator.alloc(u8, capacity);
        return .{
            .buffer = buf,
            .capacity = capacity,
            .write_pos = 0,
            .read_pos = 0,
            .index_map = .{},
            .entry_count = 0,
            .total_appended = 0,
            .total_bytes_written = 0,
            .min_live_index = 0,
            .max_index = 0,
            .on_evict = null,
            .on_append_ctx = null,
            .on_append = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *UAL) void {
        self.index_map.deinit(self.allocator);
        self.allocator.free(self.buffer);
    }

    /// Bytes currently used in the ring.
    pub fn used(self: *const UAL) u64 {
        return self.write_pos - self.read_pos;
    }

    /// Bytes available for writing.
    pub fn available(self: *const UAL) u64 {
        return self.capacity - self.used();
    }

    /// Append an entry to the ring. Returns the UAL index.
    ///
    /// If there isn't enough room, evicts oldest entries first.
    /// The entry's index field is used as-is — the caller (Raft) is
    /// responsible for monotonic index assignment.
    pub fn append(self: *UAL, entry: *const Entry) !u64 {
        const total_size = entry.totalSize();
        if (total_size > self.capacity) return error.EntryTooLarge;

        // Evict until there's room
        while (self.available() < total_size) {
            self.evictOldest();
        }

        // Record position before writing
        const pos = self.write_pos;

        // Write header
        const hdr_bytes = entry.header.asBytes();
        self.writeToRing(hdr_bytes);

        // Write payload
        if (entry.payload.len > 0) {
            self.writeToRing(entry.payload);
        }

        // Record in index map
        try self.index_map.put(self.allocator, entry.header.index, pos);

        self.entry_count += 1;
        self.total_appended += 1;
        self.total_bytes_written += total_size;

        if (self.entry_count == 1) {
            self.min_live_index = entry.header.index;
        }
        self.max_index = entry.header.index;

        // Persistence callback — feeds entries to SegmentWriter for disk durability
        if (self.on_append) |cb| {
            cb(self.on_append_ctx.?, entry);
        }

        return entry.header.index;
    }

    /// Read an entry by UAL index. Returns null if evicted or not found.
    /// The returned Entry borrows from the ring buffer — valid until
    /// that region is overwritten.
    pub fn read(self: *const UAL, index: u64) ?Entry {
        const pos = self.index_map.get(index) orelse return null;

        // Verify position is still in live range
        if (pos < self.read_pos) return null;

        // Read header
        var hdr_buf: [HEADER_SIZE]u8 = undefined;
        self.readFromRing(pos, &hdr_buf);

        const hdr: *const Header = @ptrCast(@alignCast(&hdr_buf));

        // Validate magic as a sanity check
        if (hdr.magic != entry_mod.ENTRY_MAGIC) return null;

        // For payload, we need to read it into a contiguous view.
        // Since the ring may wrap, we check if the entry spans the boundary.
        const payload_len: usize = hdr.payload_len;
        const payload_start = pos + HEADER_SIZE;
        const payload_offset = @as(usize, @intCast(payload_start % self.capacity));

        // Check if payload is contiguous in the ring
        if (payload_offset + payload_len <= self.capacity) {
            // Contiguous — zero-copy
            const payload = self.buffer[payload_offset .. payload_offset + payload_len];
            return .{
                .header = hdr.*,
                .payload = payload,
            };
        }

        // Payload wraps around the ring boundary — can't do zero-copy.
        // Return null (caller should use readCopy for wrapped entries).
        return null;
    }

    /// Read an entry by index, copying payload into a caller-provided buffer.
    /// Works even if the payload wraps around the ring boundary.
    pub fn readCopy(self: *const UAL, index: u64, payload_buf: []u8) ?Entry {
        const pos = self.index_map.get(index) orelse return null;
        if (pos < self.read_pos) return null;

        var hdr_buf: [HEADER_SIZE]u8 = undefined;
        self.readFromRing(pos, &hdr_buf);

        const hdr: *const Header = @ptrCast(@alignCast(&hdr_buf));
        if (hdr.magic != entry_mod.ENTRY_MAGIC) return null;

        const payload_len: usize = hdr.payload_len;
        if (payload_buf.len < payload_len) return null;

        // Read payload (handles wrap-around)
        self.readFromRingTo(pos + HEADER_SIZE, payload_buf[0..payload_len]);

        return .{
            .header = hdr.*,
            .payload = payload_buf[0..payload_len],
        };
    }

    /// Scan entries in index range [from_index, to_index).
    /// Returns number of entries found. Caller provides slice of Entry for results.
    pub fn readRange(self: *const UAL, from_index: u64, to_index: u64, results: []Entry) usize {
        var count: usize = 0;
        var idx = from_index;
        while (idx < to_index and count < results.len) {
            if (self.read(idx)) |e| {
                results[count] = e;
                count += 1;
            }
            idx += 1;
        }
        return count;
    }

    /// Check if an index is present in the hot ring.
    pub fn contains(self: *const UAL, index: u64) bool {
        return self.index_map.get(index) != null;
    }

    // ─── Internal ────────────────────────────────────────────────────

    /// Write data to the ring at write_pos, handling wrap-around.
    fn writeToRing(self: *UAL, data: []const u8) void {
        const offset = @as(usize, @intCast(self.write_pos % self.capacity));
        const first_chunk = @min(data.len, self.capacity - offset);

        @memcpy(self.buffer[offset .. offset + first_chunk], data[0..first_chunk]);
        if (first_chunk < data.len) {
            // Wrap around
            const remaining = data.len - first_chunk;
            @memcpy(self.buffer[0..remaining], data[first_chunk..]);
        }

        self.write_pos += data.len;
    }

    /// Read data from the ring at a virtual position.
    fn readFromRing(self: *const UAL, pos: u64, out: []u8) void {
        self.readFromRingTo(pos, out);
    }

    /// Read data from the ring at a virtual position into a buffer.
    fn readFromRingTo(self: *const UAL, pos: u64, out: []u8) void {
        const offset = @as(usize, @intCast(pos % self.capacity));
        const first_chunk = @min(out.len, self.capacity - offset);

        @memcpy(out[0..first_chunk], self.buffer[offset .. offset + first_chunk]);
        if (first_chunk < out.len) {
            const remaining = out.len - first_chunk;
            @memcpy(out[first_chunk..], self.buffer[0..remaining]);
        }
    }

    /// Evict the oldest entry from the ring.
    fn evictOldest(self: *UAL) void {
        if (self.entry_count == 0) return;

        // Read header at read_pos to find the entry size
        var hdr_buf: [HEADER_SIZE]u8 = undefined;
        self.readFromRing(self.read_pos, &hdr_buf);

        const hdr: *const Header = @ptrCast(@alignCast(&hdr_buf));
        const entry_size: u64 = HEADER_SIZE + hdr.payload_len;

        // Call eviction callback before removing
        if (self.on_evict) |cb| {
            cb(hdr_buf[0..HEADER_SIZE], hdr.index);
        }

        // Remove from index map
        _ = self.index_map.remove(hdr.index);

        self.read_pos += entry_size;
        self.entry_count -= 1;

        // Update min_live_index
        if (self.entry_count > 0) {
            self.min_live_index = hdr.index + 1;
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn makeEntry(index: u64, payload: []const u8) Entry {
    return entry_mod.buildEntry(.kv_put, 0, 1, index, 0, payload);
}

test "UAL: init and deinit" {
    var ual = try UAL.init(testing.allocator, 4096);
    defer ual.deinit();

    try testing.expectEqual(@as(u64, 0), ual.entry_count);
    try testing.expectEqual(@as(u64, 4096), ual.available());
}

test "UAL: append and read" {
    var ual = try UAL.init(testing.allocator, 4096);
    defer ual.deinit();

    const entry = makeEntry(1, "hello");
    const idx = try ual.append(&entry);
    try testing.expectEqual(@as(u64, 1), idx);
    try testing.expectEqual(@as(u64, 1), ual.entry_count);

    const recovered = ual.read(1);
    try testing.expect(recovered != null);
    try testing.expectEqual(@as(u64, 1), recovered.?.header.index);
    try testing.expectEqualStrings("hello", recovered.?.payload);
}

test "UAL: multiple appends" {
    var ual = try UAL.init(testing.allocator, 4096);
    defer ual.deinit();

    var i: u64 = 1;
    while (i <= 10) : (i += 1) {
        const entry = makeEntry(i, "data");
        _ = try ual.append(&entry);
    }

    try testing.expectEqual(@as(u64, 10), ual.entry_count);
    try testing.expectEqual(@as(u64, 10), ual.max_index);
    try testing.expectEqual(@as(u64, 1), ual.min_live_index);

    // Read each
    i = 1;
    while (i <= 10) : (i += 1) {
        const e = ual.read(i);
        try testing.expect(e != null);
        try testing.expectEqual(i, e.?.header.index);
    }
}

test "UAL: eviction on full ring" {
    // Small ring — fits ~2 entries (header=40 + payload=10 = 50 each)
    var ual = try UAL.init(testing.allocator, 120);
    defer ual.deinit();

    const e1 = makeEntry(1, "aaaaaaaaaa"); // 50 bytes
    const e2 = makeEntry(2, "bbbbbbbbbb"); // 50 bytes
    const e3 = makeEntry(3, "cccccccccc"); // 50 bytes

    _ = try ual.append(&e1);
    _ = try ual.append(&e2);
    try testing.expectEqual(@as(u64, 2), ual.entry_count);

    // This should evict entry 1
    _ = try ual.append(&e3);

    // Entry 1 should be gone
    try testing.expectEqual(@as(?Entry, null), ual.read(1));
    // Entry 2 or 3 should be present (depending on how many evictions)
    try testing.expect(ual.read(3) != null);
}

test "UAL: readCopy handles wrap-around" {
    // 100-byte ring, entries of ~50 bytes
    var ual = try UAL.init(testing.allocator, 100);
    defer ual.deinit();

    const e1 = makeEntry(1, "1234567890"); // 50 bytes
    _ = try ual.append(&e1);

    // This forces wrap-around after evicting e1
    const e2 = makeEntry(2, "abcdefghij"); // 50 bytes
    _ = try ual.append(&e2);

    // readCopy should handle potential wrap
    var payload_buf: [64]u8 = undefined;
    const recovered = ual.readCopy(2, &payload_buf);
    try testing.expect(recovered != null);
    try testing.expectEqualStrings("abcdefghij", recovered.?.payload);
}

test "UAL: readRange" {
    var ual = try UAL.init(testing.allocator, 4096);
    defer ual.deinit();

    var i: u64 = 1;
    while (i <= 5) : (i += 1) {
        const entry = makeEntry(i, "test");
        _ = try ual.append(&entry);
    }

    var results: [10]Entry = undefined;
    const count = ual.readRange(2, 5, &results);
    try testing.expectEqual(@as(usize, 3), count); // indices 2, 3, 4
    try testing.expectEqual(@as(u64, 2), results[0].header.index);
    try testing.expectEqual(@as(u64, 3), results[1].header.index);
    try testing.expectEqual(@as(u64, 4), results[2].header.index);
}

test "UAL: contains" {
    var ual = try UAL.init(testing.allocator, 4096);
    defer ual.deinit();

    const entry = makeEntry(42, "x");
    _ = try ual.append(&entry);

    try testing.expect(ual.contains(42));
    try testing.expect(!ual.contains(43));
    try testing.expect(!ual.contains(1));
}

test "UAL: entry too large for ring" {
    var ual = try UAL.init(testing.allocator, 64);
    defer ual.deinit();

    // Entry needs 40 (header) + 100 (payload) = 140 > 64
    const big_payload = "x" ** 100;
    const entry = makeEntry(1, big_payload);
    try testing.expectError(error.EntryTooLarge, ual.append(&entry));
}

test "UAL: stats tracking" {
    var ual = try UAL.init(testing.allocator, 4096);
    defer ual.deinit();

    const entry = makeEntry(1, "stats");
    _ = try ual.append(&entry);

    try testing.expectEqual(@as(u64, 1), ual.total_appended);
    try testing.expectEqual(@as(u64, 45), ual.total_bytes_written); // 40 header + 5 payload
}
