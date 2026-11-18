//! Raft hard state — what a node must not forget across a crash.
//!
//! `HARDSTATE` in each shard directory holds the node's identity, its current
//! term, the vote it cast in that term, and whether its lost-log guard is on.
//! It is rewritten (tmp → fsync → rename → directory fsync) before the node
//! grants a vote, whenever it adopts a term, and when the guard starts or
//! ends, so a restarted node can neither vote twice in one term, re-enter a
//! term it already left, nor drop a guard it has not finished.
//! Terms change per election, not per write, so the fsync never sits on the
//! write path.
//!
//! It also holds the vouch a guarded node took (`Vouch`): the config another
//! member said its group had committed, which the node hears the members of
//! until its own commit reaches that config's index. Written with the guard,
//! so a restart part way through catching up keeps the trust it held, and no
//! more: the same config, ending at the same index. Read back only while the
//! guard is on.
//!
//! Layout (little-endian):
//!
//! ```
//! magic:       u32   0x0A10_4853
//! version:     u8    2
//! flags:       u8    bit 0: lost-log (the node has not yet re-established
//!                    what it lost; see `raft.node.LostLog`)
//! reserved:    [2]u8 zero
//! node_id:     u32
//! term:        u64
//! voted_for:   u32
//! vouch_index: u64   0 for none
//! vouch_term:  u64
//! config_len:  u16   the vouched config, encoded (`membership.encode`)
//! config:      [config_len]u8
//! crc32c:      u32   over everything before it
//! ```

const std = @import("std");
const stdx = @import("stdx");
const checksum = @import("../util/checksum.zig");
const membership = @import("membership.zig");

pub const FILENAME = "HARDSTATE";
pub const MAGIC: u32 = 0x0A10_4853;
pub const VERSION: u8 = 2;
const FIXED: usize = 42;
pub const MAX_SIZE: usize = FIXED + membership.MAX_SIZE + 4;

/// A member's word for its group's committed config (the replica's), held
/// while this node's own commit is short of `index`.
pub const Vouch = struct {
    config: membership.Config = .{},
    index: u64 = 0,
    term: u64 = 0,
};

pub const DecodeError = error{ BadMagic, BadVersion, BadCrc, Truncated, BadVouch };

pub const HardState = struct {
    node_id: u32 = 0,
    term: u64 = 0,
    voted_for: u32 = 0,
    lost_log: bool = false,
    vouch: Vouch = .{},

    /// The encoded bytes, in `buf`.
    pub fn encode(self: HardState, buf: *[MAX_SIZE]u8) []const u8 {
        std.mem.writeInt(u32, buf[0..4], MAGIC, .little);
        buf[4] = VERSION;
        buf[5] = @intFromBool(self.lost_log);
        @memset(buf[6..8], 0);
        std.mem.writeInt(u32, buf[8..12], self.node_id, .little);
        std.mem.writeInt(u64, buf[12..20], self.term, .little);
        std.mem.writeInt(u32, buf[20..24], self.voted_for, .little);
        std.mem.writeInt(u64, buf[24..32], self.vouch.index, .little);
        std.mem.writeInt(u64, buf[32..40], self.vouch.term, .little);
        var cfg_buf: [membership.MAX_SIZE]u8 = undefined;
        const cfg: []const u8 = if (self.vouch.index == 0) "" else membership.encode(&self.vouch.config, &cfg_buf);
        std.mem.writeInt(u16, buf[40..42], @intCast(cfg.len), .little);
        @memcpy(buf[FIXED..][0..cfg.len], cfg);
        const end = FIXED + cfg.len;
        std.mem.writeInt(u32, buf[end..][0..4], checksum.checksum(buf[0..end]), .little);
        return buf[0 .. end + 4];
    }

    /// A file that is short, long, corrupt or holds a vouch whose config
    /// doesn't decode is refused: never read as a fresh start, never a
    /// vouch nobody gave.
    pub fn decode(bytes: []const u8) DecodeError!HardState {
        if (bytes.len < FIXED + 4) return error.Truncated;
        if (std.mem.readInt(u32, bytes[0..4], .little) != MAGIC) return error.BadMagic;
        if (bytes[4] != VERSION) return error.BadVersion;
        const cfg_len = std.mem.readInt(u16, bytes[40..42], .little);
        if (bytes.len != FIXED + cfg_len + 4) return error.Truncated;
        const end = FIXED + cfg_len;
        if (std.mem.readInt(u32, bytes[end..][0..4], .little) != checksum.checksum(bytes[0..end])) return error.BadCrc;
        var hs: HardState = .{
            .node_id = std.mem.readInt(u32, bytes[8..12], .little),
            .term = std.mem.readInt(u64, bytes[12..20], .little),
            .voted_for = std.mem.readInt(u32, bytes[20..24], .little),
            .lost_log = bytes[5] & 1 != 0,
        };
        const index = std.mem.readInt(u64, bytes[24..32], .little);
        if (index != 0) {
            hs.vouch = .{
                .config = membership.decode(bytes[FIXED..end]) orelse return error.BadVouch,
                .index = index,
                .term = std.mem.readInt(u64, bytes[32..40], .little),
            };
        } else if (cfg_len != 0) return error.BadVouch;
        return hs;
    }
};

/// Read `{dir}/HARDSTATE`. Null when the file does not exist (a fresh shard).
/// A short or corrupt file is an error, not a fresh start: silently starting
/// at term 0 is exactly the double vote the file exists to prevent.
pub fn load(dir: []const u8) !?HardState {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir, FILENAME });

    const file = stdx.fs.openFile(path, .{}) catch |err| {
        if (err == error.FileNotFound) return null;
        return err;
    };
    defer stdx.fs.closeFile(file);

    var buf: [MAX_SIZE + 1]u8 = undefined;
    const n = try stdx.fs.readAll(file, &buf);
    return try HardState.decode(buf[0..n]);
}

/// Atomically replace `{dir}/HARDSTATE`. Returns only once the bytes are
/// synced; the caller may act on the new term or vote after this returns.
pub fn save(dir: []const u8, hs: HardState) !void {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir, FILENAME });
    var tmp_buf: [std.fs.max_path_bytes]u8 = undefined;
    const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}/{s}.tmp", .{ dir, FILENAME });

    var buf: [MAX_SIZE]u8 = undefined;
    const bytes = hs.encode(&buf);

    const file = try stdx.fs.createFile(tmp_path, .{});
    errdefer stdx.fs.deleteFile(tmp_path) catch {};
    {
        defer stdx.fs.closeFile(file);
        try stdx.fs.writeAll(file, bytes);
        try stdx.fs.sync(file);
    }
    // Without the directory sync a power loss can bring back the old file,
    // and with it a term or vote this node already acted on.
    try stdx.fs.renameDurable(tmp_path, path);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "hard state: encode/decode roundtrip" {
    const hs = HardState{ .node_id = 0xDEADBEEF, .term = 42, .voted_for = 7 };
    var buf: [MAX_SIZE]u8 = undefined;
    const back = try HardState.decode(hs.encode(&buf));
    try testing.expectEqual(hs.node_id, back.node_id);
    try testing.expectEqual(hs.term, back.term);
    try testing.expectEqual(hs.voted_for, back.voted_for);
    try testing.expectEqual(@as(u64, 0), back.vouch.index);
}

test "hard state: a flipped byte is a crc error, not a term 0" {
    const hs = HardState{ .node_id = 1, .term = 9, .voted_for = 1 };
    var buf: [MAX_SIZE]u8 = undefined;
    const bytes = hs.encode(&buf);
    buf[13] ^= 0x01; // inside `term`
    try testing.expectError(error.BadCrc, HardState.decode(bytes));
    buf[13] ^= 0x01;
    buf[0] ^= 0xFF;
    try testing.expectError(error.BadMagic, HardState.decode(bytes));
}

test "hard state: a vouch comes back exactly as written, and one that's cut or garbled is refused" {
    const cfg = membership.Config.ofVoters(&.{ 1, 2, 3 });
    const hs = HardState{ .node_id = 3, .term = 20, .voted_for = 0, .lost_log = true, .vouch = .{ .config = cfg, .index = 3355, .term = 19 } };
    var buf: [MAX_SIZE]u8 = undefined;
    const bytes = hs.encode(&buf);
    const back = try HardState.decode(bytes);
    try testing.expect(back.lost_log);
    try testing.expectEqual(@as(u64, 3355), back.vouch.index);
    try testing.expectEqual(@as(u64, 19), back.vouch.term);
    var ids: [membership.MAX_MEMBERS]u32 = undefined;
    try testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, back.vouch.config.voterIds(&ids));
    // Cut short, or a byte flipped inside the vouch.
    try testing.expectError(error.Truncated, HardState.decode(bytes[0 .. bytes.len - 1]));
    try testing.expectError(error.Truncated, HardState.decode(bytes[0..20]));
    buf[FIXED + 1] ^= 0x01;
    try testing.expectError(error.BadCrc, HardState.decode(bytes));
    buf[FIXED + 1] ^= 0x01;
    // A config that doesn't decode, under a good checksum: refused, not an
    // empty vouch.
    @memset(buf[FIXED .. bytes.len - 4], 0xFF);
    std.mem.writeInt(u32, buf[bytes.len - 4 ..][0..4], checksum.checksum(buf[0 .. bytes.len - 4]), .little);
    try testing.expectError(error.BadVouch, HardState.decode(bytes));
    // Config bytes with no index: no vouch was written so.
    const none = (HardState{ .node_id = 3 }).encode(&buf);
    var odd: [MAX_SIZE]u8 = undefined;
    @memcpy(odd[0..none.len], none);
    std.mem.writeInt(u16, odd[40..42], 1, .little);
    odd[FIXED] = 0;
    std.mem.writeInt(u32, odd[FIXED + 1 ..][0..4], checksum.checksum(odd[0 .. FIXED + 1]), .little);
    try testing.expectError(error.BadVouch, HardState.decode(odd[0 .. FIXED + 5]));
}

fn tmpDirPath(tmp: *testing.TmpDir, buf: []u8) ![]const u8 {
    const real = try stdx.fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
    defer testing.allocator.free(real);
    if (real.len > buf.len) return error.NameTooLong;
    @memcpy(buf[0..real.len], real);
    return buf[0..real.len];
}

test "hard state: save then load survives, missing file is null" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmpDirPath(&tmp, &path_buf);

    try testing.expectEqual(@as(?HardState, null), try load(dir));

    const before = stdx.fs.dir_syncs.load(.monotonic);
    try save(dir, .{ .node_id = 3, .term = 5, .voted_for = 3 });
    try testing.expectEqual(before + 1, stdx.fs.dir_syncs.load(.monotonic));
    const a = (try load(dir)).?;
    try testing.expectEqual(@as(u64, 5), a.term);
    try testing.expectEqual(@as(u32, 3), a.voted_for);
    try testing.expectEqual(@as(u32, 3), a.node_id);

    // Overwrite is a replace, and no .tmp is left behind.
    try save(dir, .{ .node_id = 3, .term = 6, .voted_for = 0 });
    const b = (try load(dir)).?;
    try testing.expectEqual(@as(u64, 6), b.term);
    try testing.expectEqual(@as(u32, 0), b.voted_for);
    var tmp_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const tmp_path = try std.fmt.bufPrint(&tmp_path_buf, "{s}/{s}.tmp", .{ dir, FILENAME });
    try testing.expectError(error.FileNotFound, stdx.fs.access(tmp_path, .{}));
}

test "hard state: a short file refuses to load" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmpDirPath(&tmp, &path_buf);

    var file_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&file_buf, "{s}/{s}", .{ dir, FILENAME });
    const file = try stdx.fs.createFile(path, .{});
    try stdx.fs.writeAll(file, &@as([FIXED]u8, @splat(0)));
    stdx.fs.closeFile(file);
    try testing.expectError(error.Truncated, load(dir));
}
