//! Where a shard lost an entry or a segment without the client or the
//! operator being told. Each path is counted (`flo_shard_silent_loss_total`)
//! and logged, rate-limited per path, so none of them is silent any more.

const std = @import("std");

const log = @import("stdx").log;

pub const Path = enum {
    /// The partition refused the entry (the hot ring could not take it).
    partition_reject,
    /// A projection the router owns (KV, queue, TS) failed to apply it.
    router_error,
    /// A registry applier could not apply it (a payload that would not
    /// decode, an allocation that failed).
    applier_failed,
    /// No applier owns its type.
    no_applier,
    /// A committed entry this node could not read back to apply.
    unreadable_committed,
    /// A segment whose header would not read.
    segment_header_unreadable,
    /// A segment that would not open or failed its checksum.
    segment_unreadable,
    /// An entry that would not parse inside a segment whose checksum held.
    segment_bad_entry,
    /// An index no segment holds, below entries that are held.
    index_hole,
    /// A MANIFEST or snapshot that would not load.
    snapshot_unloadable,
};

pub const PATH_COUNT = @typeInfo(Path).@"enum".field_names.len;

/// How often each path may log a line while it keeps happening.
pub const WARN_INTERVAL_MS: u64 = 30_000;

pub const Counts = struct {
    counts: [PATH_COUNT]u64 = @splat(0),
    /// Counted since that path's last line, said with the next one.
    unlogged: [PATH_COUNT]u64 = @splat(0),
    last_line_ms: [PATH_COUNT]u64 = @splat(0),

    /// Count one loss on `path` and say why, unless that path has spoken
    /// within `WARN_INTERVAL_MS`; then the next line says how many went
    /// unsaid.
    pub fn note(self: *Counts, path: Path, now_ms: u64, shard_id: u16, comptime fmt: []const u8, args: anytype) void {
        const i = @intFromEnum(path);
        self.counts[i] += 1;
        if (self.last_line_ms[i] != 0 and now_ms -| self.last_line_ms[i] < WARN_INTERVAL_MS) {
            self.unlogged[i] += 1;
            return;
        }
        self.last_line_ms[i] = @max(now_ms, 1);
        log.err("shard {d}: lost ({s}): " ++ fmt ++ " ({d} more since the last line, {d} in all)", .{ shard_id, @tagName(path) } ++ args ++ .{ self.unlogged[i], self.counts[i] });
        self.unlogged[i] = 0;
    }

    pub fn total(self: *const Counts, path: Path) u64 {
        return self.counts[@intFromEnum(path)];
    }
};

/// Damage a shard's data directory holds: which path found it and what.
/// A shard holding it serves nothing on this node (single node) or stops
/// taking part in its group (cluster).
pub const Damage = struct {
    path: Path,
    buf: [192]u8 = undefined,
    len: usize = 0,

    pub fn init(path: Path, comptime fmt: []const u8, args: anytype) Damage {
        var d: Damage = .{ .path = path };
        // Too long for the buffer: kept cut short.
        d.len = if (std.fmt.bufPrint(&d.buf, fmt, args)) |w| w.len else |_| d.buf.len;
        return d;
    }

    /// What is wrong, for the log and the client's `unavailable` answer.
    pub fn reason(self: *const Damage) []const u8 {
        return self.buf[0..self.len];
    }
};

test "silent loss: every loss is counted, and a path speaks at most once per interval" {
    var c: Counts = .{};
    c.note(.segment_unreadable, 1_000, 3, "segment {s}", .{"a.flseg"});
    c.note(.segment_unreadable, 2_000, 3, "segment {s}", .{"b.flseg"});
    try std.testing.expectEqual(@as(u64, 2), c.total(.segment_unreadable));
    try std.testing.expectEqual(@as(u64, 1), c.unlogged[@intFromEnum(Path.segment_unreadable)]);
    c.note(.segment_unreadable, 1_000 + WARN_INTERVAL_MS, 3, "segment {s}", .{"c.flseg"});
    try std.testing.expectEqual(@as(u64, 0), c.unlogged[@intFromEnum(Path.segment_unreadable)]);
    try std.testing.expectEqual(@as(u64, 0), c.total(.index_hole));
}
