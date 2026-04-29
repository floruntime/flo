//! Session Window Assigner
//!
//! Session windows are dynamic, gap-based windows. A session window
//! starts when a record arrives and extends as long as new records
//! arrive within the gap duration. Sessions merge when they overlap.
//!
//! Unlike tumbling/sliding, session windows are per-key and can't
//! use the fixed WindowAssigner union directly. Instead, the
//! SessionWindowManager tracks active sessions per key and handles
//! merging when a new record bridges two sessions.
//!
//! Gap = 5000ms example:
//!   Record at t=1000 → session [1000, 6000)
//!   Record at t=3000 → extends to [1000, 8000)
//!   Record at t=20000 → new session [20000, 25000)

const std = @import("std");
const Allocator = std.mem.Allocator;
const TimeWindow = @import("assigner.zig").TimeWindow;

// =============================================================================
// SessionConfig
// =============================================================================

pub const SessionConfig = struct {
    /// The gap duration in milliseconds. If no record arrives within
    /// `gap_ms` of the last record, the session closes.
    gap_ms: i64,
};

// =============================================================================
// SessionWindowManager
// =============================================================================

/// Manages session windows per key. Sessions are dynamic and merge
/// when a new record falls within an existing session's gap.
pub const SessionWindowManager = struct {
    /// Active sessions per key: key → sorted list of windows
    sessions: std.StringHashMap(std.ArrayList(TimeWindow)),
    gap_ms: i64,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, gap_ms: i64) Self {
        return .{
            .sessions = std.StringHashMap(std.ArrayList(TimeWindow)).init(allocator),
            .gap_ms = gap_ms,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.sessions.deinit();
    }

    /// Add a record and return the session window it belongs to.
    /// May trigger merging of overlapping sessions.
    pub fn addRecord(self: *Self, key: []const u8, event_time_ms: i64) !TimeWindow {
        const new_window = TimeWindow{
            .start_ms = event_time_ms,
            .end_ms = event_time_ms + self.gap_ms,
        };

        const gop = try self.sessions.getOrPut(key);
        if (!gop.found_existing) {
            // New key — allocate owned copy for the map key
            gop.key_ptr.* = try self.allocator.dupe(u8, key);
            gop.value_ptr.* = .empty;
            try gop.value_ptr.append(self.allocator, new_window);
            return new_window;
        }

        // Key exists — just append the new window and merge
        var windows = gop.value_ptr;
        try windows.append(self.allocator, new_window);

        // Merge overlapping windows
        return self.mergeWindows(windows);
    }

    /// Merge all overlapping/adjacent windows in the list.
    /// Returns the window that contains the most recently added record.
    fn mergeWindows(self: *Self, windows: *std.ArrayList(TimeWindow)) TimeWindow {
        _ = self;
        if (windows.items.len <= 1) return windows.items[0];

        // Sort by start time
        std.mem.sort(TimeWindow, windows.items, {}, struct {
            fn cmp(_: void, a: TimeWindow, b: TimeWindow) bool {
                return a.start_ms < b.start_ms;
            }
        }.cmp);

        // Merge overlapping windows in-place
        var write: usize = 0;
        for (windows.items[1..]) |w| {
            if (w.start_ms <= windows.items[write].end_ms) {
                // Merge: extend end
                windows.items[write].end_ms = @max(windows.items[write].end_ms, w.end_ms);
            } else {
                write += 1;
                windows.items[write] = w;
            }
        }
        windows.shrinkRetainingCapacity(write + 1);

        // Return the last window (most recently affected)
        return windows.items[windows.items.len - 1];
    }

    /// Get all active sessions for a key.
    pub fn getSessions(self: *const Self, key: []const u8) ?[]const TimeWindow {
        if (self.sessions.get(key)) |list| {
            return list.items;
        }
        return null;
    }

    /// Fire (remove) all sessions for a key that are closed by the watermark.
    /// A session is closed when: watermark >= session.end_ms
    /// Returns the fired windows.
    pub fn fireExpiredSessions(
        self: *Self,
        key: []const u8,
        watermark_ms: i64,
        out: []TimeWindow,
    ) usize {
        const windows = self.sessions.getPtr(key) orelse return 0;

        var fired: usize = 0;
        var i: usize = 0;
        while (i < windows.items.len) {
            if (windows.items[i].end_ms <= watermark_ms) {
                if (fired < out.len) {
                    out[fired] = windows.items[i];
                    fired += 1;
                }
                _ = windows.orderedRemove(i);
            } else {
                i += 1;
            }
        }
        return fired;
    }

    /// Count total active sessions across all keys.
    pub fn totalActiveSessions(self: *const Self) usize {
        var total: usize = 0;
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            total += entry.value_ptr.items.len;
        }
        return total;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "SessionWindowManager basic session creation" {
    const allocator = std.testing.allocator;
    var mgr = SessionWindowManager.init(allocator, 5000);
    defer mgr.deinit();

    const w = try mgr.addRecord("user1", 1000);
    try std.testing.expectEqual(@as(i64, 1000), w.start_ms);
    try std.testing.expectEqual(@as(i64, 6000), w.end_ms);
    try std.testing.expectEqual(@as(usize, 1), mgr.totalActiveSessions());
}

test "SessionWindowManager extends session on nearby record" {
    const allocator = std.testing.allocator;
    var mgr = SessionWindowManager.init(allocator, 5000);
    defer mgr.deinit();

    _ = try mgr.addRecord("user1", 1000); // [1000, 6000)
    const w = try mgr.addRecord("user1", 3000); // [1000, 8000) — merged

    try std.testing.expectEqual(@as(i64, 1000), w.start_ms);
    try std.testing.expectEqual(@as(i64, 8000), w.end_ms);
    try std.testing.expectEqual(@as(usize, 1), mgr.totalActiveSessions());
}

test "SessionWindowManager creates separate session after gap" {
    const allocator = std.testing.allocator;
    var mgr = SessionWindowManager.init(allocator, 5000);
    defer mgr.deinit();

    _ = try mgr.addRecord("user1", 1000); // [1000, 6000)
    _ = try mgr.addRecord("user1", 20000); // [20000, 25000) — new session

    try std.testing.expectEqual(@as(usize, 2), mgr.totalActiveSessions());
    const sessions = mgr.getSessions("user1").?;
    try std.testing.expectEqual(@as(usize, 2), sessions.len);
}

test "SessionWindowManager fires expired sessions" {
    const allocator = std.testing.allocator;
    var mgr = SessionWindowManager.init(allocator, 5000);
    defer mgr.deinit();

    _ = try mgr.addRecord("user1", 1000); // [1000, 6000)
    _ = try mgr.addRecord("user1", 20000); // [20000, 25000)

    var fired: [8]TimeWindow = undefined;
    const count = mgr.fireExpiredSessions("user1", 7000, &fired);

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(i64, 1000), fired[0].start_ms);
    try std.testing.expectEqual(@as(usize, 1), mgr.totalActiveSessions());
}

test "SessionWindowManager different keys independent" {
    const allocator = std.testing.allocator;
    var mgr = SessionWindowManager.init(allocator, 5000);
    defer mgr.deinit();

    _ = try mgr.addRecord("user1", 1000);
    _ = try mgr.addRecord("user2", 2000);

    try std.testing.expectEqual(@as(usize, 2), mgr.totalActiveSessions());
    try std.testing.expectEqual(@as(usize, 1), mgr.getSessions("user1").?.len);
    try std.testing.expectEqual(@as(usize, 1), mgr.getSessions("user2").?.len);
}
