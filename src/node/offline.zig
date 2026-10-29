//! A stopped node's cluster group, read and repaired offline: what
//! `flo server inspect` prints and what `flo server force-members` writes.
//! Only shard 0 is replicated, so only its directory is read for those. A
//! single node's halted shard is repaired offline too (`repairSkip`).
//! Callers hold the data dir's lock for the whole command.

const std = @import("std");
const stdx = @import("stdx");
const hard_state_mod = @import("../raft/hard_state.zig");
const membership = @import("../raft/membership.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const durable_log_mod = @import("../storage/durable_log.zig");
const SegmentWriter = @import("../storage/ual/writer.zig").SegmentWriter;
const repair = @import("repair.zig");
const apply_marker = @import("apply_marker.zig");

const Entry = entry_mod.Entry;

pub const GROUP_DIR = "00000";

pub const Summary = struct {
    hard_state: ?hard_state_mod.HardState = null,
    first_index: u64 = 0,
    last_index: u64 = 0,
    last_term: u64 = 0,
    /// The last entry's stamp: an entry appended offline is stamped after it.
    last_stamp: u64 = 0,
    /// What the segments were flushed under: committed as far as this node
    /// knows.
    commit: u64 = 0,
    config_index: u64 = 0,
    config_term: u64 = 0,
    config: membership.Config = .{},
    /// The config's member ids, voters and replicas.
    members: [membership.MAX_MEMBERS]u32 = undefined,
    member_count: usize = 0,
    truncation_pending: bool = false,

    pub fn configMembers(self: *const Summary) []const u32 {
        return self.members[0..self.member_count];
    }
};

pub fn groupDir(buf: []u8, data_dir: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}/{s}", .{ data_dir, GROUP_DIR });
}

pub fn shardDir(buf: []u8, data_dir: []const u8, shard: u16) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}/{d:0>5}", .{ data_dir, shard });
}

/// Read shard 0's hard state, log range and latest config. Writes nothing.
pub fn summarize(allocator: std.mem.Allocator, data_dir: []const u8) !Summary {
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    return summarizeDir(allocator, try groupDir(&dir_buf, data_dir));
}

/// The same, for the shard whose directory is `dir`.
pub fn summarizeDir(allocator: std.mem.Allocator, dir: []const u8) !Summary {
    var segs_buf: [std.fs.max_path_bytes]u8 = undefined;
    const segs = try std.fmt.bufPrint(&segs_buf, "{s}/segs", .{dir});

    var sum: Summary = .{ .hard_state = try hard_state_mod.load(dir) };
    var intent_buf: [std.fs.max_path_bytes]u8 = undefined;
    const intent = try std.fmt.bufPrint(&intent_buf, "{s}/{s}", .{ segs, durable_log_mod.INTENT_FILENAME });
    sum.truncation_pending = if (stdx.fs.openFile(intent, .{})) |f| blk: {
        stdx.fs.closeFile(f);
        break :blk true;
    } else |_| false;

    var writer = SegmentWriter.init(allocator, 0);
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
            sum.last_stamp = e.header.timestamp_ns;
            if (e.header.entry_type != @backingInt(entry_mod.EntryType.raft_config)) continue;
            sum.config = membership.decode(e.payload) orelse continue;
            sum.member_count = sum.config.ids(&sum.members).len;
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

    var writer = SegmentWriter.init(allocator, 0);
    defer writer.deinit();
    var dl = try durable_log_mod.DurableLog.init(allocator, &writer, segs);
    defer dl.deinit();
    // A cut the server recorded but did not finish leaves the summary's
    // last index unsure; the server's next boot finishes it.
    if (sum.truncation_pending) return error.TruncationIncomplete;

    var cfg_buf: [membership.MAX_SIZE]u8 = undefined;
    const term = @max(hs.term, sum.last_term);
    const index = sum.last_index + 1;
    // The only voter; removed ids stay removed, except this one: a node
    // forced back after its removal is a member again, and a config naming
    // it both ways would not decode.
    var forced = membership.Config.ofVoters(&.{hs.node_id});
    for (sum.config.removedSlice()) |rm| {
        if (rm.id == hs.node_id) continue;
        forced.removed[forced.removed_count] = rm;
        forced.removed_count += 1;
    }
    // Stamped after the entry before it, as a leader stamps: apply halts on
    // a stamp that isn't.
    const now_ns: u64 = @intCast(@as(i64, @import("stdx").time.milliTimestamp()) * std.time.ns_per_ms);
    const stamp = @max(sum.last_stamp + 1, now_ns);
    var e = entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, term, index, stamp, membership.encode(&forced, &cfg_buf));
    e.header.crc32c = e.computeCrc();
    try writer.addEntry(&e);
    try dl.flush(index);

    var cleared = hs;
    cleared.lost_log = false;
    if (hs.lost_log) try hard_state_mod.save(dir, cleared);
}

pub const SkipError = error{ NoLog, TruncationIncomplete, NotInLog, IsSkip, NotSkippable, NotHalted };

/// Shard `dir`'s entry at `index`, its payload in `arena`.
pub fn entryAt(allocator: std.mem.Allocator, dir: []const u8, index: u64, arena: []u8) !Entry {
    var segs_buf: [std.fs.max_path_bytes]u8 = undefined;
    const segs = try std.fmt.bufPrint(&segs_buf, "{s}/segs", .{dir});
    var writer = SegmentWriter.init(allocator, 0);
    defer writer.deinit();
    var dl = try durable_log_mod.DurableLog.init(allocator, &writer, segs);
    defer dl.deinit();
    var one: [1]Entry = undefined;
    if (dl.readRange(index, &one, arena) != 1 or one[0].header.index != index) return error.NotInLog;
    return one[0];
}

/// Whether `e` is the entry the stopped shard `shard` in `dir` is halted
/// at. On a single node the majority that must have halted at it is the
/// node: its marker names the entry the shard last halted at, or whose
/// apply crashed it. Any other entry may have applied, and skipping it
/// would lose a write its client was told succeeded.
pub fn skippable(dir: []const u8, shard: u16, e: *const Entry) SkipError!void {
    switch (@as(entry_mod.EntryType, @fromBackingInt(e.header.entry_type))) {
        .repair_skip => return error.IsSkip,
        // Applied around a halt, so never what a shard halts at.
        .raft_config, .raft_noop => return error.NotSkippable,
        else => {},
    }
    const held = apply_marker.read(dir, shard);
    if (held.halt) |*h| if (h.names(e)) return;
    if (held.crash) |*c| if (c.names(e)) return;
    return error.NotHalted;
}

/// Append a repair skip of `halted` to the stopped shard `shard` in `dir`,
/// sealed as committed: its next boot passes over the entry, and so does
/// every boot after, if it is `skippable`. Returns the skip's index.
pub fn repairSkip(allocator: std.mem.Allocator, dir: []const u8, shard: u16, sum: *const Summary, halted: *const Entry, reason: []const u8) !u64 {
    if (sum.last_index == 0) return error.NoLog;
    if (sum.truncation_pending) return error.TruncationIncomplete;
    try skippable(dir, shard, halted);
    var segs_buf: [std.fs.max_path_bytes]u8 = undefined;
    const segs = try std.fmt.bufPrint(&segs_buf, "{s}/segs", .{dir});
    var writer = SegmentWriter.init(allocator, 0);
    defer writer.deinit();
    var dl = try durable_log_mod.DurableLog.init(allocator, &writer, segs);
    defer dl.deinit();

    const term = if (sum.hard_state) |hs| @max(hs.term, sum.last_term) else sum.last_term;
    const index = sum.last_index + 1;
    // Stamped after the entry before it, as a leader stamps.
    const now_ns: u64 = @intCast(@as(i64, @import("stdx").time.milliTimestamp()) * std.time.ns_per_ms);
    const stamp = @max(sum.last_stamp + 1, now_ns);
    var p: [repair.Skip.MAX_PAYLOAD]u8 = undefined;
    var e = entry_mod.buildEntry(.repair_skip, entry_mod.Flags.NONE, term, index, stamp, repair.Skip.of(halted, reason).encode(&p));
    e.header.crc32c = e.computeCrc();
    try writer.addEntry(&e);
    // Sealed with the watermark at its own index: boot knows it committed.
    try dl.flush(index);
    return index;
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
    var w = SegmentWriter.init(testing.allocator, 0);
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
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 10, ""),
        entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 1, 2, 20, membership.encode(&membership.Config.ofVoters(&.{ 1, 2, 3 }), &cfg_buf)),
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 4, 3, 30, ""),
    };
    for (&es) |*e| e.header.crc32c = e.computeCrc();
    try writeLog(dir, &es, 2);
    try hard_state_mod.save(dir, .{ .node_id = 2, .term = 5, .voted_for = 3, .lost_log = true });

    var sum = try summarize(testing.allocator, root);
    try testing.expectEqual(@as(u64, 1), sum.first_index);
    try testing.expectEqual(@as(u64, 3), sum.last_index);
    try testing.expectEqual(@as(u64, 4), sum.last_term);
    try testing.expectEqual(@as(u64, 2), sum.commit);
    try testing.expectEqual(@as(u64, 2), sum.config_index);
    try testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, sum.configMembers());

    try forceMembers(testing.allocator, root, &sum);
    var after = try summarize(testing.allocator, root);
    try testing.expectEqual(@as(u64, 4), after.last_index);
    try testing.expectEqual(@as(u64, 4), after.commit);
    try testing.expectEqual(@as(u64, 4), after.config_index);
    try testing.expectEqual(@as(u64, 5), after.config_term);
    try testing.expectEqualSlices(u32, &.{2}, after.configMembers());
    try testing.expect(!after.hard_state.?.lost_log);
    // Stamped after the entry before it, so applying it doesn't halt.
    try testing.expect(after.last_stamp > 30);
}

test "offline: force-members refuses a data dir with no log" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try stdx.fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
    defer testing.allocator.free(root);
    var sum = try summarize(testing.allocator, root);
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

test "offline: a skip is only of the entry the shard is halted at, and never of a config, a no-op or a skip" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try stdx.fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
    defer testing.allocator.free(root);
    var cfg_buf: [membership.MAX_SIZE]u8 = undefined;
    var es: [4]Entry = .{
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 10, ""),
        entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 1, 2, 20, membership.encode(&membership.Config.ofVoters(&.{1}), &cfg_buf)),
        entry_mod.buildEntry(.repair_skip, entry_mod.Flags.NONE, 1, 3, 30, ""),
        entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, 4, 40, "x"),
    };
    for (&es) |*e| e.header.crc32c = e.computeCrc();
    try testing.expectError(error.NotHalted, skippable(root, 0, &es[3]));
    var m = try apply_marker.Marker.open(root, 0);
    defer m.close();
    // Even halted there.
    for (es[0..3], [_]anyerror{ error.NotSkippable, error.NotSkippable, error.IsSkip }) |*e, want| {
        m.halted(e.header.index, e.header.term, e.header.crc32c);
        try testing.expectError(want, skippable(root, 0, e));
    }
    m.halted(4, 1, es[3].header.crc32c);
    try skippable(root, 0, &es[3]);
    try testing.expectError(error.NotHalted, skippable(root, 1, &es[3]));
    m.halted(4, 2, es[3].header.crc32c);
    try testing.expectError(error.NotHalted, skippable(root, 0, &es[3]));
}
