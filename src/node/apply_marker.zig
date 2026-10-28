//! The entry a shard was applying when the process crashed.
//!
//! An entry whose apply panics (an overflow, an out-of-bounds index in a
//! safe build) or faults panics on every replica, and again on every replay:
//! a crash loop. The panic and fault handlers (the root's `panic` and
//! `debug.handleSegfault`) call `recordCrash`, which writes the entry this
//! thread was applying to its shard's `apply.marker`. The next boot halts
//! the shard at that entry instead of applying it again.
//!
//! Only the crashing thread's own shard is named: every shard runs on its
//! own thread, and one shard's panic kills them all. A crash outside apply
//! names nothing.
//!
//! The write is one pwrite into a file opened at boot, with no fsync: a
//! crash leaves the page cache, and a power loss is not an apply bug. A
//! record cut short or never written fails its checksum and reads as none.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const entry_mod = @import("../storage/ual/entry.zig");
const checksum = @import("../util/checksum.zig");

const log = std.log.scoped(.apply_marker);

pub const FILENAME = "apply.marker";

const MAGIC: u32 = 0x4d4f4c46; // "FLOM"
const RECORD_SIZE = 128;
const BUILD_MAX = 64;

/// This build, as a crash names it. A release always builds from a clean
/// tree, so its describe tells builds apart. Two dev builds of one dirty
/// tree share a name; one retrying the other's crash is harmless, since it
/// crashes again and writes a fresh record. A build with no version (no
/// git, or an image built with the default `FLO_VERSION=dev`) is named
/// version "dev", so an upgrade between two of them halts at the entry instead of
/// retrying it.
pub const build_name: []const u8 = blk: {
    const full = std.fmt.comptimePrint("{s} zig {s} {t}", .{ build_options.version, builtin.zig_version_string, builtin.mode });
    break :blk full[0..@min(full.len, BUILD_MAX)];
};

/// A crash a marker recorded.
pub const Crash = struct {
    shard: u16,
    index: u64,
    term: u64,
    entry_crc: u32,
    build_buf: [BUILD_MAX]u8 = @splat(0),
    build_len: u8 = 0,

    pub fn build(self: *const Crash) []const u8 {
        return self.build_buf[0..self.build_len];
    }

    /// The entry the crash was applying: the same index, term and checksum.
    pub fn names(self: *const Crash, e: *const entry_mod.Entry) bool {
        return self.index == e.header.index and self.term == e.header.term and self.entry_crc == e.header.crc32c;
    }

    fn encode(self: *const Crash, out: *[RECORD_SIZE]u8) void {
        out.* = @splat(0);
        std.mem.writeInt(u32, out[0..4], MAGIC, .little);
        std.mem.writeInt(u16, out[8..10], self.shard, .little);
        out[10] = self.build_len;
        std.mem.writeInt(u32, out[12..16], self.entry_crc, .little);
        std.mem.writeInt(u64, out[16..24], self.index, .little);
        std.mem.writeInt(u64, out[24..32], self.term, .little);
        @memcpy(out[32..][0..BUILD_MAX], &self.build_buf);
        std.mem.writeInt(u32, out[4..8], checksum.checksum(out[8..]), .little);
    }

    fn decode(bytes: *const [RECORD_SIZE]u8) ?Crash {
        if (std.mem.readInt(u32, bytes[0..4], .little) != MAGIC) return null;
        if (std.mem.readInt(u32, bytes[4..8], .little) != checksum.checksum(bytes[8..])) return null;
        var c: Crash = .{
            .shard = std.mem.readInt(u16, bytes[8..10], .little),
            .build_len = bytes[10],
            .entry_crc = std.mem.readInt(u32, bytes[12..16], .little),
            .index = std.mem.readInt(u64, bytes[16..24], .little),
            .term = std.mem.readInt(u64, bytes[24..32], .little),
        };
        if (c.build_len > BUILD_MAX) return null;
        @memcpy(&c.build_buf, bytes[32..][0..BUILD_MAX]);
        return c;
    }
};

/// A shard's marker file, and the crash it held at boot.
pub const Marker = struct {
    fd: std.posix.fd_t = -1,
    shard: u16 = 0,
    /// The crash to halt at, from this build; cleared once a different
    /// entry is found at its index.
    crashed: ?Crash = null,

    /// Open `<shard_dir>/apply.marker` and take what it records. A crash
    /// another build recorded is cleared, so this build tries the entry
    /// again: it may carry the fix.
    pub fn open(shard_dir: []const u8, shard: u16) !Marker {
        var buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try std.fmt.bufPrintSentinel(&buf, "{s}/{s}", .{ shard_dir, FILENAME }, 0);
        const fd = std.c.open(path, .{ .ACCMODE = .RDWR, .CREAT = true, .CLOEXEC = true }, @as(std.c.mode_t, 0o644));
        if (fd < 0) return error.MarkerOpenFailed;
        var m: Marker = .{ .fd = fd, .shard = shard };
        var rec: [RECORD_SIZE]u8 = undefined;
        if (std.c.pread(fd, &rec, rec.len, 0) != rec.len) return m;
        const c = Crash.decode(&rec) orelse return m;
        if (c.shard != shard) {
            m.clear();
        } else if (!std.mem.eql(u8, c.build(), build_name)) {
            log.warn("shard {d}: entry {d} crashed build {s}; retrying it on {s}", .{ shard, c.index, c.build(), build_name });
            m.clear();
        } else {
            m.crashed = c;
        }
        return m;
    }

    pub fn close(self: *Marker) void {
        if (self.fd >= 0) _ = std.c.close(self.fd);
        self.fd = -1;
    }

    /// Whether `e` is the entry that crashed this build. An entry of another
    /// term or checksum at that index (a later leader replaced it) clears
    /// the marker and applies.
    pub fn crashedOn(self: *Marker, e: *const entry_mod.Entry) bool {
        const c = &(self.crashed orelse return false);
        if (c.index != e.header.index) return false;
        if (c.names(e)) return true;
        log.info("shard {d}: entry {d} is not the one that crashed (term {d}, was {d}); applying it", .{ self.shard, c.index, e.header.term, c.term });
        self.clear();
        return false;
    }

    /// Clear a crash on an entry below `first`, the first this log holds:
    /// it was applied, or lies under a snapshot, and no apply reaches it
    /// again.
    pub fn dropBelow(self: *Marker, first: u64) void {
        const c = &(self.crashed orelse return);
        if (c.index >= first) return;
        log.info("shard {d}: entry {d} crashed this node, but the log now starts at {d}; nothing applies it again", .{ self.shard, c.index, first });
        self.clear();
    }

    fn clear(self: *Marker) void {
        self.crashed = null;
        const zero: [RECORD_SIZE]u8 = @splat(0);
        _ = std.c.pwrite(self.fd, &zero, zero.len, 0);
    }
};

const Applying = struct {
    fd: std.posix.fd_t,
    shard: u16,
    index: u64,
    term: u64,
    entry_crc: u32,
};

/// What this thread is applying. A thread-local, never a shard's field:
/// replay runs on a boot worker thread, and the handler must name only the
/// thread that crashed.
threadlocal var applying: ?Applying = null;
/// Set while recording, so a fault in the handler doesn't record again.
threadlocal var recording: bool = false;

/// Mark `e` as this thread's apply until `end`. Returns what to restore.
pub fn begin(marker: *const Marker, e: *const entry_mod.Entry) ?Applying {
    const prev = applying;
    if (marker.fd >= 0) applying = .{ .fd = marker.fd, .shard = marker.shard, .index = e.header.index, .term = e.header.term, .entry_crc = e.header.crc32c };
    return prev;
}

pub fn end(prev: ?Applying) void {
    applying = prev;
}

/// Called from the panic and fault handlers. Writes the marker if this
/// thread is applying an entry, and nothing else: no allocation, no
/// formatting, and a failed write is ignored, since the crash goes on as
/// it would.
pub fn recordCrash() void {
    if (recording) return;
    recording = true;
    const a = applying orelse return;
    var c: Crash = .{ .shard = a.shard, .index = a.index, .term = a.term, .entry_crc = a.entry_crc, .build_len = @intCast(build_name.len) };
    @memcpy(c.build_buf[0..build_name.len], build_name);
    var rec: [RECORD_SIZE]u8 = undefined;
    c.encode(&rec);
    _ = std.c.pwrite(a.fd, &rec, rec.len, 0);
}

const testing = std.testing;

fn testEntry(index: u64, term: u64) entry_mod.Entry {
    var e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, term, index, 1, "x");
    e.header.crc32c = e.computeCrc();
    return e;
}

/// Write what a crash on `e` would have, as another build or shard may.
pub fn writeCrashForTest(dir: []const u8, shard: u16, e: *const entry_mod.Entry, build: []const u8) !void {
    var m = try Marker.open(dir, shard);
    defer m.close();
    var c: Crash = .{ .shard = shard, .index = e.header.index, .term = e.header.term, .entry_crc = e.header.crc32c, .build_len = @intCast(build.len) };
    @memcpy(c.build_buf[0..build.len], build);
    var rec: [RECORD_SIZE]u8 = undefined;
    c.encode(&rec);
    try testing.expectEqual(@as(isize, RECORD_SIZE), std.c.pwrite(m.fd, &rec, rec.len, 0));
}

/// Relabel the record in `shard_dir` as `build`'s, as an upgrade finds it.
pub fn relabelForTest(shard_dir: []const u8, build: []const u8) !void {
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrintSentinel(&buf, "{s}/{s}", .{ shard_dir, FILENAME }, 0);
    const fd = std.c.open(path, .{ .ACCMODE = .RDWR, .CLOEXEC = true });
    if (fd < 0) return error.NoRecord;
    defer _ = std.c.close(fd);
    var rec: [RECORD_SIZE]u8 = undefined;
    if (std.c.pread(fd, &rec, rec.len, 0) != rec.len) return error.NoRecord;
    var c = Crash.decode(&rec) orelse return error.NoRecord;
    c.build_buf = @splat(0);
    c.build_len = @intCast(build.len);
    @memcpy(c.build_buf[0..build.len], build);
    c.encode(&rec);
    if (std.c.pwrite(fd, &rec, rec.len, 0) != rec.len) return error.WriteFailed;
}

/// Whether the marker file holds a record, whatever build wrote it.
fn holdsRecord(m: *const Marker) bool {
    var rec: [RECORD_SIZE]u8 = undefined;
    if (std.c.pread(m.fd, &rec, rec.len, 0) != rec.len) return false;
    return Crash.decode(&rec) != null;
}

fn tmpPath(tmp: *std.testing.TmpDir, buf: []u8) ![]const u8 {
    const real = try @import("stdx").fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
    defer testing.allocator.free(real);
    return std.fmt.bufPrint(buf, "{s}", .{real});
}

test "apply marker: a crash while applying is recorded for its shard, and read back at the next open" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmpPath(&tmp, &pbuf);
    const e = testEntry(7, 3);
    {
        var m = try Marker.open(dir, 2);
        defer m.close();
        try testing.expect(m.crashed == null);
        const prev = begin(&m, &e);
        defer end(prev);
        recordCrash();
        recording = false;
    }
    var m = try Marker.open(dir, 2);
    defer m.close();
    const c = m.crashed orelse return error.NotRecorded;
    try testing.expect(c.names(&e));
    try testing.expectEqualStrings(build_name, c.build());
    try testing.expect(m.crashedOn(&e));
}

test "apply marker: a crash outside apply records nothing, and neither does a write that fails" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmpPath(&tmp, &pbuf);
    {
        var m = try Marker.open(dir, 0);
        defer m.close();
        recordCrash();
        recording = false;
    }
    {
        const m = try Marker.open(dir, 0);
        try testing.expect(m.crashed == null);
        // The file closed under it: the handler's write fails and is let go.
        const e = testEntry(4, 1);
        const prev = begin(&m, &e);
        _ = std.c.close(m.fd);
        recordCrash();
        recording = false;
        end(prev);
    }
    var m = try Marker.open(dir, 0);
    defer m.close();
    try testing.expect(m.crashed == null);
}

test "apply marker: a torn, foreign or other-shard record reads as none" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmpPath(&tmp, &pbuf);
    const e = testEntry(9, 2);
    try writeCrashForTest(dir, 1, &e, build_name);
    {
        // Shard 0's marker file never names shard 1.
        var m = try Marker.open(dir, 0);
        defer m.close();
        try testing.expect(m.crashed == null);
    }
    try writeCrashForTest(dir, 1, &e, build_name);
    {
        var m = try Marker.open(dir, 1);
        defer m.close();
        // One flipped byte: the checksum fails.
        var b: [1]u8 = undefined;
        _ = std.c.pread(m.fd, &b, 1, 40);
        b[0] ^= 0xff;
        _ = std.c.pwrite(m.fd, &b, 1, 40);
    }
    var m = try Marker.open(dir, 1);
    defer m.close();
    try testing.expect(m.crashed == null);
}

test "apply marker: a crash another build recorded is cleared, so this build tries the entry again" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmpPath(&tmp, &pbuf);
    const e = testEntry(5, 1);
    try writeCrashForTest(dir, 0, &e, "v0.0.1 zig 0.17.0 ReleaseSafe");
    {
        var m = try Marker.open(dir, 0);
        defer m.close();
        try testing.expect(m.crashed == null);
        try testing.expect(!m.crashedOn(&e));
        // Cleared, not just passed over.
        try testing.expect(!holdsRecord(&m));
    }
    var m = try Marker.open(dir, 0);
    defer m.close();
    try testing.expect(m.crashed == null);
}

test "apply marker: another entry at the crashed index clears the marker and applies" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmpPath(&tmp, &pbuf);
    const crashed = testEntry(6, 2);
    try writeCrashForTest(dir, 0, &crashed, build_name);
    {
        var m = try Marker.open(dir, 0);
        defer m.close();
        try testing.expect(!m.crashedOn(&testEntry(5, 2)));
        try testing.expect(m.crashed != null);
        // A later leader's entry at 6, in term 3.
        try testing.expect(!m.crashedOn(&testEntry(6, 3)));
        try testing.expect(m.crashed == null);
    }
    var m = try Marker.open(dir, 0);
    defer m.close();
    try testing.expect(m.crashed == null);
}

test "apply marker: a crash below the log's first entry is cleared, and one at it is kept" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmpPath(&tmp, &pbuf);
    try writeCrashForTest(dir, 0, &testEntry(6, 2), build_name);
    {
        var m = try Marker.open(dir, 0);
        defer m.close();
        m.dropBelow(6);
        try testing.expect(m.crashed != null);
        m.dropBelow(7);
        try testing.expect(m.crashed == null);
        try testing.expect(!holdsRecord(&m));
    }
    var m = try Marker.open(dir, 0);
    defer m.close();
    try testing.expect(m.crashed == null);
}

test "apply marker: a crash on one thread never names the entry another thread is applying" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmpPath(&tmp, &pbuf);
    const e = testEntry(3, 1);
    {
        var m = try Marker.open(dir, 0);
        defer m.close();
        const prev = begin(&m, &e);
        defer end(prev);
        // Another shard's thread crashes outside apply, mid-way through this
        // one's.
        const t = try std.Thread.spawn(.{}, struct {
            fn run() void {
                recordCrash();
            }
        }.run, .{});
        t.join();
    }
    var m = try Marker.open(dir, 0);
    defer m.close();
    try testing.expect(m.crashed == null);
}

test "apply marker: a fault while recording keeps the first record" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmpPath(&tmp, &pbuf);
    const first = testEntry(3, 1);
    const second = testEntry(4, 1);
    {
        var m = try Marker.open(dir, 0);
        defer m.close();
        const outer = begin(&m, &first);
        defer end(outer);
        recordCrash();
        // The handler itself faults, mid-way through another entry.
        const inner = begin(&m, &second);
        defer end(inner);
        recordCrash();
        recording = false;
    }
    var m = try Marker.open(dir, 0);
    defer m.close();
    try testing.expect(m.crashed.?.names(&first));
}
