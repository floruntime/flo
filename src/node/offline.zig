//! A stopped node's cluster group, read and repaired offline: what
//! `flo server inspect` prints and what `flo server force-members` writes.
//! Only shard 0 is replicated, so only its directory is read. Callers hold
//! the data dir's lock for the whole command.

const std = @import("std");
const stdx = @import("stdx");
const hard_state_mod = @import("../raft/hard_state.zig");
const membership = @import("../raft/membership.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const durable_log_mod = @import("../storage/durable_log.zig");
const SegmentWriter = @import("../storage/ual/writer.zig").SegmentWriter;
const ShardManifest = @import("shard_manifest.zig").ShardManifest;

const Entry = entry_mod.Entry;

pub const GROUP_DIR = "00000";

pub const Summary = struct {
    hard_state: ?hard_state_mod.HardState = null,
    first_index: u64 = 0,
    last_index: u64 = 0,
    last_term: u64 = 0,
    /// What the segments were flushed under: committed as far as this node
    /// knows.
    commit: u64 = 0,
    config_index: u64 = 0,
    config_term: u64 = 0,
    members: [membership.MAX_MEMBERS]u32 = undefined,
    member_count: usize = 0,
    /// The latest snapshot's file name; its entries are not in the segments.
    snapshot: ?[]const u8 = null,
    truncation_pending: bool = false,

    pub fn deinit(self: *Summary, allocator: std.mem.Allocator) void {
        if (self.snapshot) |s| allocator.free(s);
    }

    pub fn configMembers(self: *const Summary) []const u32 {
        return self.members[0..self.member_count];
    }
};

pub fn groupDir(buf: []u8, data_dir: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}/{s}", .{ data_dir, GROUP_DIR });
}

/// Read shard 0's hard state, log range and latest config. Writes nothing.
pub fn summarize(allocator: std.mem.Allocator, data_dir: []const u8) !Summary {
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try groupDir(&dir_buf, data_dir);
    var segs_buf: [std.fs.max_path_bytes]u8 = undefined;
    const segs = try std.fmt.bufPrint(&segs_buf, "{s}/segs", .{dir});

    var sum: Summary = .{ .hard_state = try hard_state_mod.load(dir) };
    errdefer sum.deinit(allocator);
    if (try ShardManifest.load(allocator, dir)) |sm_val| {
        var sm = sm_val;
        defer sm.deinit(allocator);
        if (sm.latest_snapshot) |s| sum.snapshot = try allocator.dupe(u8, s);
    }
    var intent_buf: [std.fs.max_path_bytes]u8 = undefined;
    const intent = try std.fmt.bufPrint(&intent_buf, "{s}/{s}", .{ segs, durable_log_mod.INTENT_FILENAME });
    sum.truncation_pending = if (stdx.fs.openFile(intent, .{})) |f| blk: {
        stdx.fs.closeFile(f);
        break :blk true;
    } else |_| false;

    var writer = SegmentWriter.init(allocator, 0, .none);
    defer writer.deinit();
    var dl = try durable_log_mod.DurableLog.init(allocator, &writer, segs);
    defer dl.deinit();
    const files = dl.segments() catch |err| switch (err) {
        error.FileNotFound => return sum,
        else => return err,
    };
    if (files.len == 0) return sum;
    sum.first_index = std.math.maxInt(u64);
    for (files) |f| {
        sum.first_index = @min(sum.first_index, f.first_index);
        sum.last_index = @max(sum.last_index, f.last_index);
        sum.commit = @max(sum.commit, f.commit_index_at_seal);
    }

    // Walk the log for its last entry's term and its latest config.
    const arena = try allocator.alloc(u8, 4 * 1024 * 1024);
    defer allocator.free(arena);
    var batch: [256]Entry = undefined;
    var idx = sum.first_index;
    while (idx <= sum.last_index) {
        const n = dl.readRange(idx, &batch, arena);
        if (n == 0) return error.LogUnreadable;
        for (batch[0..n]) |*e| {
            sum.last_term = e.header.term;
            if (e.header.entry_type != @intFromEnum(entry_mod.EntryType.raft_config)) continue;
            const members = membership.decode(e.payload, &sum.members) orelse continue;
            sum.member_count = members.len;
            sum.config_index = e.header.index;
            sum.config_term = e.header.term;
        }
        idx += n;
    }
    return sum;
}

pub const ForceError = error{ NoLog, NoHardState, TruncationIncomplete };

/// Make this node the group's only voter: append a config naming only
/// it, sealed as committed, then clear a lost-log flag. The config is on
/// disk (file and directory synced) before the flag is cleared, so a crash
/// between the two leaves the node guarded, not unguarded without the
/// config.
pub fn forceMembers(allocator: std.mem.Allocator, data_dir: []const u8, sum: *const Summary) !void {
    if (sum.last_index == 0) return error.NoLog;
    const hs = sum.hard_state orelse return error.NoHardState;
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try groupDir(&dir_buf, data_dir);
    var segs_buf: [std.fs.max_path_bytes]u8 = undefined;
    const segs = try std.fmt.bufPrint(&segs_buf, "{s}/segs", .{dir});

    var writer = SegmentWriter.init(allocator, 0, .none);
    defer writer.deinit();
    var dl = try durable_log_mod.DurableLog.init(allocator, &writer, segs);
    defer dl.deinit();
    // A cut the server recorded but did not finish leaves the summary's
    // last index unsure; the server's next boot finishes it.
    if (sum.truncation_pending) return error.TruncationIncomplete;

    var cfg_buf: [membership.MAX_SIZE]u8 = undefined;
    const term = @max(hs.term, sum.last_term);
    const index = sum.last_index + 1;
    var e = entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, term, index, 0, membership.encode(&.{hs.node_id}, &cfg_buf));
    e.header.crc32c = e.computeCrc();
    try writer.addEntry(&e);
    try dl.flush(index);

    var cleared = hs;
    cleared.lost_log = false;
    if (hs.lost_log) try hard_state_mod.save(dir, cleared);
}

// ─── Retired secrets ─────────────────────────────────────────────────────────

/// Secrets `force-members` retired, one per line: the hex SHA-256
/// fingerprint and the Unix time it was retired. A node never links with a
/// retired secret again, at any boot.
pub const RETIRED_FILENAME = "RETIRED_SECRET";

pub fn fingerprint(secret: []const u8) [32]u8 {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    h.update("flo-retired-secret\x00");
    h.update(secret);
    return h.finalResult();
}

/// When `secret` was retired in this data dir, or null.
pub fn retiredAt(allocator: std.mem.Allocator, data_dir: []const u8, secret: []const u8) !?u64 {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ data_dir, RETIRED_FILENAME });
    const text = stdx.fs.readFileAlloc(allocator, path, 64 * 1024) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer allocator.free(text);
    const want = std.fmt.bytesToHex(fingerprint(secret), .lower);
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        var parts = std.mem.splitScalar(u8, line, ' ');
        const fp = parts.next() orelse continue;
        if (!std.mem.eql(u8, fp, &want)) continue;
        return std.fmt.parseInt(u64, parts.next() orelse "0", 10) catch 0;
    }
    return null;
}

/// Record `secret` as retired at `now_s`, durably (tmp, fsync, rename,
/// directory fsync), keeping what was retired before.
pub fn retire(allocator: std.mem.Allocator, data_dir: []const u8, secret: []const u8, now_s: u64) !void {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ data_dir, RETIRED_FILENAME });
    var tmp_buf: [std.fs.max_path_bytes]u8 = undefined;
    const tmp = try std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{path});
    const old = stdx.fs.readFileAlloc(allocator, path, 64 * 1024) catch |err| switch (err) {
        error.FileNotFound => try allocator.dupe(u8, ""),
        else => return err,
    };
    defer allocator.free(old);
    const line = try std.fmt.allocPrint(allocator, "{s} {d}\n", .{ &std.fmt.bytesToHex(fingerprint(secret), .lower), now_s });
    defer allocator.free(line);

    const file = try stdx.fs.createFile(tmp, .{});
    errdefer stdx.fs.deleteFile(tmp) catch {};
    {
        defer stdx.fs.closeFile(file);
        try stdx.fs.writeAll(file, old);
        try stdx.fs.writeAll(file, line);
        try stdx.fs.sync(file);
    }
    try stdx.fs.renameDurable(tmp, path);
}

/// `YYYY-MM-DD` of a Unix time, for messages.
pub fn formatDate(buf: *[10]u8, unix_s: u64) []const u8 {
    const day = (std.time.epoch.EpochSeconds{ .secs = unix_s }).getEpochDay();
    const yd = day.calculateYearDay();
    const md = yd.calculateMonthDay();
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ yd.year, md.month.numeric(), md.day_index + 1 }) catch buf[0..0];
}

// ─── Tests ───────────────────────────────────────────────────────────────────

const testing = std.testing;

fn writeLog(dir: []const u8, entries: []const Entry, commit: u64) !void {
    var segs_buf: [std.fs.max_path_bytes]u8 = undefined;
    const segs = try std.fmt.bufPrint(&segs_buf, "{s}/segs", .{dir});
    try stdx.fs.makePath(segs);
    var w = SegmentWriter.init(testing.allocator, 0, .none);
    defer w.deinit();
    for (entries) |*e| try w.addEntry(e);
    w.commit_index_at_seal = commit;
    try w.writeToFile(segs);
}

test "offline: inspect reads the log's range, term and latest config; force-members makes this node the only voter" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try stdx.fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
    defer testing.allocator.free(root);
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try groupDir(&dir_buf, root);
    try stdx.fs.makePath(dir);

    var cfg_buf: [membership.MAX_SIZE]u8 = undefined;
    var es: [3]Entry = .{
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 0, ""),
        entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 1, 2, 0, membership.encode(&.{ 1, 2, 3 }, &cfg_buf)),
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 4, 3, 0, ""),
    };
    for (&es) |*e| e.header.crc32c = e.computeCrc();
    try writeLog(dir, &es, 2);
    try hard_state_mod.save(dir, .{ .node_id = 2, .term = 5, .voted_for = 3, .lost_log = true });

    var sum = try summarize(testing.allocator, root);
    defer sum.deinit(testing.allocator);
    try testing.expectEqual(@as(u64, 1), sum.first_index);
    try testing.expectEqual(@as(u64, 3), sum.last_index);
    try testing.expectEqual(@as(u64, 4), sum.last_term);
    try testing.expectEqual(@as(u64, 2), sum.commit);
    try testing.expectEqual(@as(u64, 2), sum.config_index);
    try testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, sum.configMembers());

    try forceMembers(testing.allocator, root, &sum);
    var after = try summarize(testing.allocator, root);
    defer after.deinit(testing.allocator);
    try testing.expectEqual(@as(u64, 4), after.last_index);
    try testing.expectEqual(@as(u64, 4), after.commit);
    try testing.expectEqual(@as(u64, 4), after.config_index);
    try testing.expectEqual(@as(u64, 5), after.config_term);
    try testing.expectEqualSlices(u32, &.{2}, after.configMembers());
    try testing.expect(!after.hard_state.?.lost_log);
}

test "offline: force-members refuses a data dir with no log" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try stdx.fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
    defer testing.allocator.free(root);
    var sum = try summarize(testing.allocator, root);
    defer sum.deinit(testing.allocator);
    try testing.expectError(error.NoLog, forceMembers(testing.allocator, root, &sum));
}

test "offline: a retired secret is recognised, with when, and others are not" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try stdx.fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
    defer testing.allocator.free(root);
    try testing.expectEqual(@as(?u64, null), try retiredAt(testing.allocator, root, "old"));
    try retire(testing.allocator, root, "old", 1_760_000_000);
    try retire(testing.allocator, root, "older", 1_700_000_000);
    try testing.expectEqual(@as(?u64, 1_760_000_000), try retiredAt(testing.allocator, root, "old"));
    try testing.expectEqual(@as(?u64, 1_700_000_000), try retiredAt(testing.allocator, root, "older"));
    try testing.expectEqual(@as(?u64, null), try retiredAt(testing.allocator, root, "new"));
    var d: [10]u8 = undefined;
    try testing.expectEqualStrings("2025-10-09", formatDate(&d, 1_760_000_000));
}
