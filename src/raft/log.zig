//! Raft Log — backed by the Unified Append Log (UAL)
//!
//! Wraps the UAL to provide Raft-specific semantics:
//! - Monotonic index/term tracking
//! - `truncateAfter()` for log conflict resolution
//! - A run-length term index for log-matching even after ring eviction
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

/// One run of consecutive indices sharing a term: `[first_index, next.first_index)`.
pub const TermRun = struct {
    first_index: u64,
    term: u64,
};

pub const RaftLog = struct {
    ual: UAL,
    allocator: Allocator,

    /// Logical bounds — may differ from UAL physical bounds after truncation.
    last_idx: u64,
    first_idx: u64,

    /// Term index, run-length encoded and ascending by `first_index`. Terms
    /// change per election, not per entry, so this stays a few dozen runs for
    /// a log of any length. Survives ring eviction so log-matching works for
    /// entries only a segment still holds.
    term_runs: std.ArrayListUnmanaged(TermRun),

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
            .term_runs = .empty,
            .snapshot_index = 0,
            .snapshot_term = 0,
        };
    }

    pub fn deinit(self: *RaftLog) void {
        self.term_runs.deinit(self.allocator);
        self.ual.deinit();
    }

    // ── Append ──────────────────────────────────────────────────────────

    /// Append an entry to the log. The entry's index must equal `lastIndex() + 1`,
    /// or `snapshot_index + 1` for the first entry after a snapshot.
    /// Returns the assigned index.
    pub fn append(self: *RaftLog, e: *const Entry) !u64 {
        const expected = if (self.last_idx == 0) self.snapshot_index + 1 else self.last_idx + 1;
        if (e.header.index != expected) return error.IndexGap;

        // Extend the term index first: a failed run allocation must not leave
        // an entry in the ring that `entryTerm` cannot answer for.
        const runs = self.term_runs.items;
        if (runs.len == 0 or runs[runs.len - 1].term != e.header.term) {
            try self.term_runs.append(self.allocator, .{ .first_index = e.header.index, .term = e.header.term });
        }
        errdefer self.trimRunsAbove(e.header.index - 1);

        const idx = try self.ual.append(e);

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

    /// Get the term for a given index. Answered from the term index (survives
    /// eviction), or the snapshot term at the snapshot index.
    pub fn entryTerm(self: *const RaftLog, index: u64) ?u64 {
        if (index == 0) return @as(u64, 0);
        if (index == self.snapshot_index) return self.snapshot_term;
        if (index > self.last_idx) return null;
        return self.runTerm(index);
    }

    /// Read a range of entries starting from `start_index`.
    /// Returns the number of entries written to `buf`. Payloads are copied into
    /// `payload_arena` (wrap-safe), so the returned entries outlive ring writes
    /// and no boundary-wrapping entry is silently skipped — a gap in a
    /// replication batch would diverge followers.
    pub fn getRange(self: *const RaftLog, start_index: u64, buf: []Entry, payload_arena: []u8) usize {
        if (start_index > self.last_idx) return 0;
        // readRangeCopy uses an exclusive upper bound, so add 1
        const end_exclusive = @min(start_index + buf.len, self.last_idx + 1);
        return self.ual.readRangeCopy(start_index, end_exclusive, buf, payload_arena);
    }

    // ── Truncation ──────────────────────────────────────────────────────

    /// Truncate all entries after `after_index`. Used for log conflict resolution
    /// when a follower receives entries that conflict with its log.
    ///
    /// After truncation, `lastIndex() == after_index`.
    pub fn truncateAfter(self: *RaftLog, after_index: u64) void {
        if (after_index >= self.last_idx) return;

        self.ual.truncateAfter(after_index);
        self.trimRunsAbove(after_index);
        self.last_idx = after_index;
    }

    /// Drop term runs that start above `index`.
    fn trimRunsAbove(self: *RaftLog, index: u64) void {
        while (self.term_runs.items.len > 0) {
            const last = self.term_runs.items[self.term_runs.items.len - 1];
            if (last.first_index <= index) break;
            _ = self.term_runs.pop();
        }
    }

    /// Term of the run covering `index`, or null if no run does.
    fn runTerm(self: *const RaftLog, index: u64) ?u64 {
        const runs = self.term_runs.items;
        if (runs.len == 0 or index < runs[0].first_index) return null;
        // Binary search for the last run with first_index <= index.
        var lo: usize = 0;
        var hi: usize = runs.len;
        while (hi - lo > 1) {
            const mid = lo + (hi - lo) / 2;
            if (runs[mid].first_index <= index) lo = mid else hi = mid;
        }
        return runs[lo].term;
    }

    // ── Queries ─────────────────────────────────────────────────────────

    /// The highest log index (0 if empty).
    pub fn lastIndex(self: *const RaftLog) u64 {
        return self.last_idx;
    }

    /// The term of the last log entry (0 if empty).
    pub fn lastTerm(self: *const RaftLog) u64 {
        if (self.last_idx == 0) return self.snapshot_term;
        return self.runTerm(self.last_idx) orelse self.snapshot_term;
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

    pub fn termRunCount(self: *const RaftLog) usize {
        return self.term_runs.items.len;
    }

    // ── Snapshot Support ────────────────────────────────────────────────

    /// Record that a snapshot was taken at the given index and term.
    /// Entries at or before the snapshot index may be safely compacted.
    pub fn recordSnapshot(self: *RaftLog, index: u64, term: u64) void {
        self.snapshot_index = index;
        self.snapshot_term = term;
    }

    /// Forget every entry and continue from `index` as if a snapshot covered
    /// it: the next append must be `index + 1`, and `entryTerm(index)` is
    /// `term`. Used at boot when the durable history has a hole.
    pub fn resetToSnapshot(self: *RaftLog, index: u64, term: u64) void {
        self.ual.truncateAfter(0);
        self.term_runs.clearRetainingCapacity();
        self.last_idx = 0;
        self.first_idx = 0;
        self.recordSnapshot(index, term);
    }

    /// Check if log-matching is possible for a given (index, term) pair.
    /// Returns true if we can verify the term at that index (either from
    /// the term index, the snapshot, or index 0).
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
    try testing.expectEqual(@as(usize, 2), log.termRunCount());
}

test "raft log: term index is run-length, not per entry" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 1 << 16);
    defer log.deinit();

    // 300 entries over terms 1,1,3,7 → 3 runs (a repeated term opens no new
    // run), and every index still answers.
    var idx: u64 = 1;
    const terms = [_]u64{ 1, 1, 3, 7 };
    for (terms) |t| {
        for (0..75) |_| {
            var e = makeEntry(.kv_put, idx, t, "x");
            _ = try log.append(&e);
            idx += 1;
        }
    }
    try testing.expectEqual(@as(usize, 3), log.termRunCount());
    try testing.expectEqual(@as(u64, 1), log.entryTerm(1).?);
    try testing.expectEqual(@as(u64, 1), log.entryTerm(150).?);
    try testing.expectEqual(@as(u64, 3), log.entryTerm(151).?);
    try testing.expectEqual(@as(u64, 3), log.entryTerm(225).?);
    try testing.expectEqual(@as(u64, 7), log.entryTerm(226).?);
    try testing.expectEqual(@as(u64, 7), log.entryTerm(300).?);
    try testing.expect(log.entryTerm(301) == null);
    try testing.expectEqual(@as(u64, 7), log.lastTerm());
}

test "raft log: term lookup survives ring eviction" {
    const allocator = testing.allocator;

    // A ring that holds only a few entries; the term index must still answer
    // for everything ever appended.
    var log = try RaftLog.init(allocator, 512);
    defer log.deinit();

    for (1..41) |i| {
        var e = makeEntry(.kv_put, @intCast(i), if (i < 20) 1 else 2, "0123456789" ** 4);
        _ = try log.append(&e);
    }
    try testing.expect(log.getEntry(1) == null); // evicted
    try testing.expectEqual(@as(u64, 1), log.entryTerm(1).?);
    try testing.expectEqual(@as(u64, 1), log.entryTerm(19).?);
    try testing.expectEqual(@as(u64, 2), log.entryTerm(20).?);
    try testing.expect(log.matchesTerm(5, 1));
    try testing.expect(!log.matchesTerm(5, 2));
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
    try testing.expectEqualStrings("new-term2", log.getEntry(2).?.payload);
}

test "raft log: truncation inside a term run keeps the run's earlier indices" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 8192);
    defer log.deinit();

    for (1..3) |i| {
        var e = makeEntry(.kv_put, @intCast(i), 1, "a");
        _ = try log.append(&e);
    }
    for (3..8) |i| {
        var e = makeEntry(.kv_put, @intCast(i), 2, "b");
        _ = try log.append(&e);
    }
    // Cut in the middle of the term-2 run: 3-4 stay term 2, 5-7 vanish.
    log.truncateAfter(4);
    try testing.expectEqual(@as(u64, 2), log.entryTerm(4).?);
    try testing.expectEqual(@as(u64, 2), log.entryTerm(3).?);
    try testing.expect(log.entryTerm(5) == null);
    try testing.expectEqual(@as(usize, 2), log.termRunCount());

    // Re-appending in the same term does not open a new run; a new term does.
    var e5 = makeEntry(.kv_put, 5, 2, "c");
    _ = try log.append(&e5);
    try testing.expectEqual(@as(usize, 2), log.termRunCount());
    var e6 = makeEntry(.kv_put, 6, 3, "d");
    _ = try log.append(&e6);
    try testing.expectEqual(@as(usize, 3), log.termRunCount());
    try testing.expectEqual(@as(u64, 3), log.lastTerm());

    // Cutting a whole run away restores the previous run as the tip.
    log.truncateAfter(5);
    try testing.expectEqual(@as(u64, 2), log.lastTerm());
    try testing.expectEqual(@as(usize, 2), log.termRunCount());
}

test "raft log: truncate then refill then evict does not wedge the ring" {
    const allocator = testing.allocator;

    // Small ring, ~40-byte payloads: the ring holds about six entries.
    var log = try RaftLog.init(allocator, 512);
    defer log.deinit();

    for (1..6) |i| {
        var e = makeEntry(.kv_put, @intCast(i), 1, "0123456789" ** 4);
        _ = try log.append(&e);
    }
    log.truncateAfter(2);
    // Conflict resolution then re-fills 3.. in a new term and keeps going
    // past the ring's capacity, which forces eviction through the region
    // the truncation reclaimed.
    for (3..40) |i| {
        var e = makeEntry(.kv_put, @intCast(i), 2, "abcdefghij" ** 4);
        _ = try log.append(&e);
    }
    try testing.expectEqual(@as(u64, 39), log.lastIndex());
    try testing.expectEqual(@as(u64, 2), log.lastTerm());
    var buf: [128]u8 = undefined;
    const tip = log.getEntryCopy(39, &buf) orelse return error.TipUnreadable;
    try testing.expectEqualStrings("abcdefghij" ** 4, tip.payload);
    try testing.expectEqual(@as(u64, 1), log.entryTerm(2).?);
    try testing.expectEqual(@as(u64, 2), log.entryTerm(3).?);
    try testing.expectEqual(log.ual.entry_count * (entry_mod.HEADER_SIZE + 40), log.ual.used());
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

test "raft log: an empty log after a snapshot continues from the snapshot index" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 4096);
    defer log.deinit();

    log.recordSnapshot(10, 4);
    var e1 = makeEntry(.kv_put, 1, 5, "data");
    try testing.expectError(error.IndexGap, log.append(&e1));

    var e11 = makeEntry(.kv_put, 11, 5, "data");
    _ = try log.append(&e11);
    try testing.expectEqual(@as(u64, 11), log.lastIndex());
    try testing.expectEqual(@as(u64, 5), log.lastTerm());
    try testing.expect(log.matchesTerm(10, 4));
    try testing.expect(log.matchesTerm(11, 5));
    try testing.expect(log.entryTerm(9) == null);
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

    // entryTerm still works for snapshot index via the term index
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
    var arena: [4096]u8 = undefined;
    const count = log.getRange(2, buf[0..3], &arena);
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

test "raft log: resetToSnapshot forgets the log and continues past the hole" {
    const allocator = testing.allocator;

    var log = try RaftLog.init(allocator, 8192);
    defer log.deinit();
    for (1..4) |i| {
        var e = makeEntry(.kv_put, @intCast(i), 1, "a");
        _ = try log.append(&e);
    }
    var e7 = makeEntry(.kv_put, 7, 2, "b");
    try testing.expectError(error.IndexGap, log.append(&e7));

    log.resetToSnapshot(6, log.lastTerm());
    try testing.expect(log.isEmpty());
    try testing.expect(log.getEntry(2) == null);
    try testing.expectEqual(@as(u64, 0), log.ual.used());
    _ = try log.append(&e7);
    try testing.expectEqual(@as(u64, 7), log.lastIndex());
    try testing.expectEqual(@as(u64, 2), log.lastTerm());
    try testing.expect(log.matchesTerm(6, 1));
    try testing.expect(log.entryTerm(3) == null);
    try testing.expectEqual(@as(usize, 1), log.termRunCount());
}
