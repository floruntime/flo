//! The leader duties a shard is holding back, and since when: what the
//! held-duty gauge, its oldest age and the 30 s summary read.

const std = @import("std");
const log = @import("stdx").log;

pub const Duty = enum { processing, stream_triggers, schedules, workflow_steps, action_expire, queue_expire, group_expire, stream_retention };

pub const Reason = enum {
    shard_halted,
    sink_halted,

    fn text(r: Reason) []const u8 {
        return switch (r) {
            .shard_halted => "the shard is halted",
            .sink_halted => "a sink's shard is halted",
        };
    }
};

/// How often a shard holding duties says so.
pub const SUMMARY_MS: u64 = 30_000;

pub const Duties = struct {
    /// When each was first held, in monotonic ms; 0 while it isn't.
    since_ms: [N]u64 = @splat(0),
    reason: [N]Reason = @splat(.shard_halted),
    summary_ms: u64 = 0,

    const N = @typeInfo(Duty).@"enum".field_names.len;

    /// Whether duty `d` acted this tick or was held back, and why.
    pub fn note(self: *Duties, d: Duty, held_for: ?Reason, now_ms: u64) void {
        const i = @backingInt(d);
        const why = held_for orelse {
            self.since_ms[i] = 0;
            return;
        };
        if (self.since_ms[i] == 0) self.since_ms[i] = @max(now_ms, 1);
        self.reason[i] = why;
    }

    pub fn clear(self: *Duties) void {
        self.since_ms = @splat(0);
    }

    pub fn held(self: *const Duties) u32 {
        var n: u32 = 0;
        for (self.since_ms) |s| n += @intFromBool(s != 0);
        return n;
    }

    /// How long the longest-held duty has waited.
    pub fn oldestMs(self: *const Duties, now_ms: u64) u64 {
        var oldest: u64 = 0;
        for (self.since_ms) |s| if (s != 0) {
            oldest = @max(oldest, now_ms -| s);
        };
        return oldest;
    }

    /// One line naming what's held, at once and then every `SUMMARY_MS`
    /// while any is.
    pub fn summarize(self: *Duties, shard_id: u16, now_ms: u64) void {
        if (self.held() == 0) {
            self.summary_ms = 0;
            return;
        }
        if (self.summary_ms != 0 and now_ms -| self.summary_ms < SUMMARY_MS) return;
        self.summary_ms = @max(now_ms, 1);
        var buf: [512]u8 = undefined;
        var w: std.Io.Writer = .fixed(&buf);
        var first = true;
        for (self.since_ms, self.reason, 0..) |s, r, i| if (s != 0) {
            w.print("{s}{t} for {d} s ({s})", .{ if (first) "" else ", ", @as(Duty, @fromBackingInt(@intCast(i))), (now_ms -| s) / 1000, r.text() }) catch break;
            first = false;
        };
        log.warn("shard {d}: {d} leader duties held: {s}", .{ shard_id, self.held(), w.buffered() });
    }
};

test "duties: held from the first tick it's held, cleared when it acts" {
    var d: Duties = .{};
    d.note(.schedules, .shard_halted, 1_000);
    d.note(.schedules, .shard_halted, 5_000);
    d.note(.processing, .sink_halted, 4_000);
    try std.testing.expectEqual(@as(u32, 2), d.held());
    try std.testing.expectEqual(@as(u64, 9_000), d.oldestMs(10_000));
    d.note(.schedules, null, 11_000);
    try std.testing.expectEqual(@as(u32, 1), d.held());
    try std.testing.expectEqual(@as(u64, 7_000), d.oldestMs(11_000));
    d.clear();
    try std.testing.expectEqual(@as(u32, 0), d.held());
}

test "duties: the summary comes at once, then not again for 30 s" {
    var d: Duties = .{};
    d.note(.action_expire, .shard_halted, 1);
    d.summarize(0, 1);
    try std.testing.expectEqual(@as(u64, 1), d.summary_ms);
    d.summarize(0, SUMMARY_MS - 1);
    try std.testing.expectEqual(@as(u64, 1), d.summary_ms);
    d.summarize(0, SUMMARY_MS + 1);
    try std.testing.expectEqual(@as(u64, SUMMARY_MS + 1), d.summary_ms);
    d.note(.action_expire, null, SUMMARY_MS + 2);
    d.summarize(0, SUMMARY_MS + 2);
    try std.testing.expectEqual(@as(u64, 0), d.summary_ms);
}
