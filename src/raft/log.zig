//! Raft Log — backed by the Unified Append Log (UAL)
//!
//! Wraps the UAL to provide Raft-specific semantics:
//! - Monotonic index/term tracking
//! - `truncateAfter()` for log conflict resolution
//! - Term cache for log-matching even after ring eviction
//! - `lastIndex()` / `lastTerm()` for election and replication
//!
//! The UAL is the Raft log — there is no separate WAL. Entries carry
//! term and index in their headers. The Raft log layer adds truncation,
//! term lookup, and index management on top.

const std = @import("std");
const Allocator = std.mem.Allocator;
const entry_mod = @import("../storage/ual/entry.zig");
const ual_mod = @import("../storage/ual/ual.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const UAL = ual_mod.UAL;

// ═══════════════════════════════════════════════════════════════════════════════
// RaftLog
// ═══════════════════════════════════════════════════════════════════════════════

pub const RaftLog = struct {
    ual: UAL,
    allocator: Allocator,

    /// Logical bounds — may differ from UAL physical bounds after truncation.
    last_idx: u64,
    first_idx: u64,

    /// Term cache — survives ring eviction so log-matching can still work.
    /// Maps UAL index → Raft term.
    term_cache: std.AutoHashMapUnmanaged(u64, u64),

    /// Snapshot state — after snapshot install, entries before this are gone.
    snapshot_index: u64,
    snapshot_term: u64,

    // ── Construction ────────────────────────────────────────────────────

    pub fn init(allocator: Allocator, capacity: usize) !RaftLog {
        var ual = try UAL.init(allocator, capacity, 0);
        errdefer ual.deinit();

        return .{
            .ual = ual,
            .allocator = allocator,
            .last_idx = 0,
            .first_idx = 0,
            .term_cache = .{},
            .snapshot_index = 0,
            .snapshot_term = 0,
        };
    }

    pub fn deinit(self: *RaftLog) void {
        self.term_cache.deinit(self.allocator);
        self.ual.deinit();
    }

    // ── Append ──────────────────────────────────────────────────────────

    /// Append an entry to the log. The entry's index must equal `lastIndex() + 1`.
    /// Returns the assigned index.
    pub fn append(self: *RaftLog, e: *const Entry) !u64 {
        const expected = self.last_idx + 1;
        if (e.header.index != expected) return error.IndexGap;

        const idx = try self.ual.append(e);

        // Cache the term for fast lookup
        try self.term_cache.put(self.allocator, idx, e.header.term);

        if (self.first_idx == 0) {
            self.first_idx = idx;
        }
        self.last_idx = idx;

        return idx;
    }

    // ── Read ────────────────────────────────────────────────────────────

    /// Get an entry by index. Returns null if not in the hot ring.
    pub fn getEntry(self: *const RaftLog, index: u64) ?Entry {
        if (index > self.last_idx or index < self.first_idx) return null;
        return self.ual.read(index);
    }

    /// Get an entry with copy (handles wrap-around in ring buffer).
    pub fn getEntryCopy(self: *const RaftLog, index: u64, payload_buf: []u8) ?Entry {
        if (index > self.last_idx or index < self.first_idx) return null;
        return self.ual.readCopy(index, payload_buf);
    }

    /// Get the term for a given index. Uses the term cache (survives eviction).
    /// Returns the snapshot term if index == snapshot_index.
    pub fn entryTerm(self: *const RaftLog, index: u64) ?u64 {
        if (index == 0) return @as(u64, 0);
        if (index == self.snapshot_index) return self.snapshot_term;
        if (index > self.last_idx) return null;
        return self.term_cache.get(index);
    }

    /// Read a range of entries starting from `start_index`.
    /// Returns the number of entries written to `buf`.
    pub fn getRange(self: *const RaftLog, start_index: u64, buf: []Entry) usize {
        if (start_index > self.last_idx) return 0;
        // UAL.readRange uses exclusive upper bound, so add 1
        const end_exclusive = @min(start_index + buf.len, self.last_idx + 1);
        return self.ual.readRange(start_index, end_exclusive, buf);
    }

    // ── Truncation ──────────────────────────────────────────────────────

    /// Truncate all entries after `after_index`. Used for log conflict resolution
    /// when a follower receives entries that conflict with its log.
    ///
    /// After truncation, `lastIndex() == after_index`.
    pub fn truncateAfter(self: *RaftLog, after_index: u64) void {
        if (after_index >= self.last_idx) return;

        // Remove entries from UAL index_map and term_cache
        var idx = after_index + 1;
        while (idx <= self.last_idx) : (idx += 1) {
            _ = self.ual.index_map.remove(idx);
            _ = self.term_cache.remove(idx);
        }

        // Adjust counters
        const removed = self.last_idx - after_index;
        self.last_idx = after_index;
        self.ual.max_index = after_index;
        if (self.ual.entry_count >= removed) {
            self.ual.entry_count -= removed;
        }
    }

    // ── Queries ─────────────────────────────────────────────────────────

    /// The highest log index (0 if empty).
    pub fn lastIndex(self: *const RaftLog) u64 {
        return self.last_idx;
    }

    /// The term of the last log entry (0 if empty).
    pub fn lastTerm(self: *const RaftLog) u64 {
        if (self.last_idx == 0) return 0;
        return self.term_cache.get(self.last_idx) orelse self.snapshot_term;
    }

    /// Number of entries in the log (logical count, not UAL physical count).
    pub fn len(self: *const RaftLog) u64 {
        if (self.last_idx == 0) return 0;
        return self.last_idx - self.first_idx + 1;
    }

    /// Whether the log is empty.
    pub fn isEmpty(self: *const RaftLog) bool {
        return self.last_idx == 0;
    }

    /// Whether the log contains an entry at the given index.
    pub fn contains(self: *const RaftLog, index: u64) bool {
        return index >= self.first_idx and index <= self.last_idx and self.ual.contains(index);
    }

    // ── Snapshot Support ────────────────────────────────────────────────

    /// Record that a snapshot was taken at the given index and term.
    /// Entries at or before the snapshot index may be safely compacted.
    pub fn recordSnapshot(self: *RaftLog, index: u64, term: u64) void {
        self.snapshot_index = index;
        self.snapshot_term = term;
    }

    /// Check if log-matching is possible for a given (index, term) pair.
    /// Returns true if we can verify the term at that index (either from
    /// the term cache, the snapshot, or index 0).
    pub fn matchesTerm(self: *const RaftLog, index: u64, term: u64) bool {
        if (index == 0 and term == 0) return true;
        const actual_term = self.entryTerm(index) orelse return false;
        return actual_term == term;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn makeEntry(entry_type: EntryType, index: u64, term: u64, payload: []const u8) Entry {
    var e = entry_mod.buildEntry(entry_type, entry_mod.Flags.NONE, term, index, 0, payload);
    e.header.crc32c = e.computeCrc();
    return e;
}

test "raft log: init empty" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 4096);
    defer log.deinit();

    try testing.expectEqual(@as(u64, 0), log.lastIndex());
    try testing.expectEqual(@as(u64, 0), log.lastTerm());
    try testing.expect(log.isEmpty());
    try testing.expectEqual(@as(u64, 0), log.len());
}

test "raft log: append and read" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 8192);
    defer log.deinit();

    var e1 = makeEntry(.kv_put, 1, 1, "key1val1");
    const idx1 = try log.append(&e1);
    try testing.expectEqual(@as(u64, 1), idx1);

    var e2 = makeEntry(.kv_put, 2, 1, "key2val2");
    const idx2 = try log.append(&e2);
    try testing.expectEqual(@as(u64, 2), idx2);

    try testing.expectEqual(@as(u64, 2), log.lastIndex());
    try testing.expectEqual(@as(u64, 1), log.lastTerm());
    try testing.expectEqual(@as(u64, 2), log.len());
    try testing.expect(!log.isEmpty());

    // Read back
    const read1 = log.getEntry(1).?;
    try testing.expectEqual(@as(u64, 1), read1.header.index);
    try testing.expectEqual(@as(u64, 1), read1.header.term);
}

test "raft log: term tracking across entries" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 8192);
    defer log.deinit();

    var e1 = makeEntry(.kv_put, 1, 1, "data");
    _ = try log.append(&e1);

    var e2 = makeEntry(.kv_put, 2, 1, "data");
    _ = try log.append(&e2);

    var e3 = makeEntry(.kv_put, 3, 2, "data"); // new term
    _ = try log.append(&e3);

    try testing.expectEqual(@as(u64, 2), log.lastTerm());

    // Term lookup
    try testing.expectEqual(@as(u64, 1), log.entryTerm(1).?);
    try testing.expectEqual(@as(u64, 1), log.entryTerm(2).?);
    try testing.expectEqual(@as(u64, 2), log.entryTerm(3).?);
    try testing.expect(log.entryTerm(4) == null); // doesn't exist
}

test "raft log: truncateAfter" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 8192);
    defer log.deinit();

    // Append 5 entries in term 1
    for (1..6) |i| {
        var e = makeEntry(.kv_put, @intCast(i), 1, "data");
        _ = try log.append(&e);
    }
    try testing.expectEqual(@as(u64, 5), log.lastIndex());

    // Truncate after index 3
    log.truncateAfter(3);
    try testing.expectEqual(@as(u64, 3), log.lastIndex());
    try testing.expectEqual(@as(u64, 1), log.lastTerm());

    // Entries 4 and 5 are gone
    try testing.expect(log.getEntry(4) == null);
    try testing.expect(log.getEntry(5) == null);
    try testing.expect(log.entryTerm(4) == null);
    try testing.expect(log.entryTerm(5) == null);

    // Entries 1-3 still accessible
    try testing.expect(log.getEntry(3) != null);
    try testing.expectEqual(@as(u64, 1), log.entryTerm(3).?);
}

test "raft log: truncate then append" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 8192);
    defer log.deinit();

    for (1..4) |i| {
        var e = makeEntry(.kv_put, @intCast(i), 1, "old");
        _ = try log.append(&e);
    }

    // Truncate after 1, then append new entries in term 2
    log.truncateAfter(1);
    try testing.expectEqual(@as(u64, 1), log.lastIndex());

    var e2 = makeEntry(.kv_put, 2, 2, "new-term2");
    _ = try log.append(&e2);

    var e3 = makeEntry(.stream_append, 3, 2, "new-stream");
    _ = try log.append(&e3);

    try testing.expectEqual(@as(u64, 3), log.lastIndex());
    try testing.expectEqual(@as(u64, 2), log.lastTerm());
    try testing.expectEqual(@as(u64, 2), log.entryTerm(2).?);
}

test "raft log: append rejects index gap" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 4096);
    defer log.deinit();

    var e1 = makeEntry(.kv_put, 1, 1, "data");
    _ = try log.append(&e1);

    // Try to append index 3 (skipping 2) — should fail
    var e3 = makeEntry(.kv_put, 3, 1, "data");
    const result = log.append(&e3);
    try testing.expectError(error.IndexGap, result);
}

test "raft log: matchesTerm" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 8192);
    defer log.deinit();

    var e1 = makeEntry(.kv_put, 1, 1, "data");
    _ = try log.append(&e1);

    var e2 = makeEntry(.kv_put, 2, 2, "data");
    _ = try log.append(&e2);

    // Index 0, term 0 always matches (empty log base case)
    try testing.expect(log.matchesTerm(0, 0));

    // Correct matches
    try testing.expect(log.matchesTerm(1, 1));
    try testing.expect(log.matchesTerm(2, 2));

    // Wrong term
    try testing.expect(!log.matchesTerm(1, 2));
    try testing.expect(!log.matchesTerm(2, 1));

    // Unknown index
    try testing.expect(!log.matchesTerm(3, 1));
}

test "raft log: snapshot recording" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 8192);
    defer log.deinit();

    for (1..6) |i| {
        var e = makeEntry(.kv_put, @intCast(i), 1, "data");
        _ = try log.append(&e);
    }

    log.recordSnapshot(3, 1);
    try testing.expectEqual(@as(u64, 3), log.snapshot_index);
    try testing.expectEqual(@as(u64, 1), log.snapshot_term);

    // entryTerm still works for snapshot index via cache
    try testing.expectEqual(@as(u64, 1), log.entryTerm(3).?);
}

test "raft log: contains" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 4096);
    defer log.deinit();

    try testing.expect(!log.contains(1));

    var e1 = makeEntry(.kv_put, 1, 1, "data");
    _ = try log.append(&e1);

    try testing.expect(log.contains(1));
    try testing.expect(!log.contains(2));
}

test "raft log: raft_noop entry type" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 4096);
    defer log.deinit();

    // Leader election noop
    var noop = makeEntry(.raft_noop, 1, 3, "");
    _ = try log.append(&noop);

    try testing.expectEqual(@as(u64, 1), log.lastIndex());
    try testing.expectEqual(@as(u64, 3), log.lastTerm());

    const read = log.getEntry(1).?;
    try testing.expectEqual(@as(u8, @intFromEnum(EntryType.raft_noop)), read.header.entry_type);
}

test "raft log: getRange" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 16384);
    defer log.deinit();

    for (1..6) |i| {
        var e = makeEntry(.kv_put, @intCast(i), 1, "data");
        _ = try log.append(&e);
    }

    var buf: [10]Entry = undefined;
    const count = log.getRange(2, buf[0..3]);
    try testing.expect(count >= 2); // at least entries 2 and 3
}

test "raft log: truncate noop when after >= lastIndex" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 4096);
    defer log.deinit();

    var e1 = makeEntry(.kv_put, 1, 1, "data");
    _ = try log.append(&e1);

    // Truncate after 5 — noop since lastIndex is 1
    log.truncateAfter(5);
    try testing.expectEqual(@as(u64, 1), log.lastIndex());

    // Truncate after 1 — noop since lastIndex is 1
    log.truncateAfter(1);
    try testing.expectEqual(@as(u64, 1), log.lastIndex());
}
