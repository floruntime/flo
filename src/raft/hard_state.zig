//! Raft hard state — what a node must not forget across a crash.
//!
//! `HARDSTATE` in each shard directory holds the node's identity, its current
//! term and the vote it cast in that term. It is rewritten (tmp → fsync →
//! rename) before the node grants a vote, and whenever it adopts a term, so a
//! restarted node can neither vote twice in one term nor re-enter a term it
//! already left.
//! Terms change per election, not per write, so the fsync never sits on the
//! write path.
//!
//! Layout (28 bytes, little-endian):
//!
//! ```
//! magic:      u32   0x0A10_4853
//! version:    u8    1
//! reserved:   [3]u8 zero
//! node_id:    u32
//! term:       u64
//! voted_for:  u32
//! crc32c:     u32   over bytes [0..24]
//! ```

const std = @import("std");
const stdx = @import("stdx");
const checksum = @import("../util/checksum.zig");

pub const FILENAME = "HARDSTATE";
pub const MAGIC: u32 = 0x0A10_4853;
pub const VERSION: u8 = 1;
pub const SIZE: usize = 28;

pub const DecodeError = error{ BadMagic, BadVersion, BadCrc };

pub const HardState = struct {
    node_id: u32 = 0,
    term: u64 = 0,
    voted_for: u32 = 0,

    pub fn encode(self: HardState, buf: *[SIZE]u8) void {
        std.mem.writeInt(u32, buf[0..4], MAGIC, .little);
        buf[4] = VERSION;
        @memset(buf[5..8], 0);
        std.mem.writeInt(u32, buf[8..12], self.node_id, .little);
        std.mem.writeInt(u64, buf[12..20], self.term, .little);
        std.mem.writeInt(u32, buf[20..24], self.voted_for, .little);
        std.mem.writeInt(u32, buf[24..28], checksum.checksum(buf[0..24]), .little);
    }

    pub fn decode(buf: *const [SIZE]u8) DecodeError!HardState {
        if (std.mem.readInt(u32, buf[0..4], .little) != MAGIC) return error.BadMagic;
        if (buf[4] != VERSION) return error.BadVersion;
        if (std.mem.readInt(u32, buf[24..28], .little) != checksum.checksum(buf[0..24])) return error.BadCrc;
        return .{
            .node_id = std.mem.readInt(u32, buf[8..12], .little),
            .term = std.mem.readInt(u64, buf[12..20], .little),
            .voted_for = std.mem.readInt(u32, buf[20..24], .little),
        };
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

    var buf: [SIZE]u8 = undefined;
    const n = try stdx.fs.readAll(file, &buf);
    if (n != SIZE) return error.Truncated;
    return try HardState.decode(&buf);
}

/// Atomically replace `{dir}/HARDSTATE`. Returns only once the bytes are
/// synced; the caller may act on the new term or vote after this returns.
pub fn save(dir: []const u8, hs: HardState) !void {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir, FILENAME });
    var tmp_buf: [std.fs.max_path_bytes]u8 = undefined;
    const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}/{s}.tmp", .{ dir, FILENAME });

    var bytes: [SIZE]u8 = undefined;
    hs.encode(&bytes);

    const file = try stdx.fs.createFile(tmp_path, .{});
    errdefer stdx.fs.deleteFile(tmp_path) catch {};
    {
        defer stdx.fs.closeFile(file);
        try stdx.fs.writeAll(file, &bytes);
        try stdx.fs.sync(file);
    }
    try stdx.fs.rename(tmp_path, path);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "hard state: encode/decode roundtrip" {
    const hs = HardState{ .node_id = 0xDEADBEEF, .term = 42, .voted_for = 7 };
    var buf: [SIZE]u8 = undefined;
    hs.encode(&buf);
    const back = try HardState.decode(&buf);
    try testing.expectEqual(hs.node_id, back.node_id);
    try testing.expectEqual(hs.term, back.term);
    try testing.expectEqual(hs.voted_for, back.voted_for);
}

test "hard state: a flipped byte is a crc error, not a term 0" {
    const hs = HardState{ .node_id = 1, .term = 9, .voted_for = 1 };
    var buf: [SIZE]u8 = undefined;
    hs.encode(&buf);
    buf[13] ^= 0x01; // inside `term`
    try testing.expectError(error.BadCrc, HardState.decode(&buf));
    buf[13] ^= 0x01;
    buf[0] ^= 0xFF;
    try testing.expectError(error.BadMagic, HardState.decode(&buf));
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

    try save(dir, .{ .node_id = 3, .term = 5, .voted_for = 3 });
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
    try stdx.fs.writeAll(file, &[_]u8{0} ** (SIZE - 4));
    stdx.fs.closeFile(file);
    try testing.expectError(error.Truncated, load(dir));
}
