//! How much memory this node may use: the host's RAM, or its cgroup's
//! limit when that is lower (a container), and where the figure came from.
//!
//! It is this node's alone. Leader-local sizing may read it; nothing that
//! every replica must agree on may, since two nodes of one cluster can
//! read different figures.

const std = @import("std");
const builtin = @import("builtin");
const stdx = @import("stdx");
const log = stdx.log;

const is_darwin = switch (builtin.target.os.tag) {
    .macos, .ios, .tvos, .watchos, .visionos => true,
    else => false,
};

/// Where a figure came from.
pub const Source = enum {
    /// The cgroup v2 `memory.max` of this process's cgroup.
    cgroup_v2,
    /// The cgroup v1 `memory.limit_in_bytes` of this process's memory cgroup.
    cgroup_v1,
    /// `MemTotal` in `/proc/meminfo`.
    meminfo,
    /// The `hw.memsize` sysctl.
    sysctl,
    /// None of them could be read: `FALLBACK_BYTES`.
    fallback,
};

/// What a node assumes when it can read no figure at all.
pub const FALLBACK_BYTES: u64 = 8 << 30;

pub const Memory = struct {
    bytes: u64,
    source: Source,

    /// The figure and its source, for a boot log: "16384 MiB (cgroup v2 memory.max)".
    pub fn describe(m: Memory, buf: []u8) []const u8 {
        const from = switch (m.source) {
            .cgroup_v2 => "cgroup v2 memory.max",
            .cgroup_v1 => "cgroup v1 memory.limit_in_bytes",
            .meminfo => "/proc/meminfo MemTotal",
            .sysctl => "sysctl hw.memsize",
            .fallback => "none could be read; assumed",
        };
        return std.fmt.bufPrint(buf, "{d} MiB ({s})", .{ m.bytes >> 20, from }) catch from;
    }
};

/// This node's memory, read from the live system. Logs when no figure can
/// be read and the fallback is assumed.
pub fn detect() Memory {
    const m = detectAt("", sysctlBytes());
    if (m.source == .fallback) {
        var buf: [96]u8 = undefined;
        log.warn("host memory: no cgroup limit, /proc/meminfo or hw.memsize could be read; assuming {s}", .{m.describe(&buf)});
    }
    return m;
}

/// `detect`, with every file read under `root` ("" for the live system) and
/// the sysctl's figure, if any, given.
pub fn detectAt(root: []const u8, sysctl: ?u64) Memory {
    const host: ?Memory = if (meminfoBytes(root)) |b|
        .{ .bytes = b, .source = .meminfo }
    else if (sysctl) |b|
        (if (b > 0) Memory{ .bytes = b, .source = .sysctl } else null)
    else
        null;
    const limit = cgroupLimit(root);
    if (limit) |l| if (host == null or l.bytes < host.?.bytes) return l;
    return host orelse .{ .bytes = FALLBACK_BYTES, .source = .fallback };
}

fn sysctlBytes() ?u64 {
    if (comptime !is_darwin) return null;
    var val: u64 = 0;
    var len: usize = @sizeOf(u64);
    if (std.c.sysctlbyname("hw.memsize", @ptrCast(&val), &len, null, 0) != 0) return null;
    return val;
}

/// `MemTotal` in bytes, or null.
fn meminfoBytes(root: []const u8) ?u64 {
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    var buf: [4096]u8 = undefined;
    const text = read(&pb, &buf, root, "/proc/meminfo") orelse return null;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, "MemTotal:")) continue;
        var words = std.mem.tokenizeAny(u8, line["MemTotal:".len..], " \t");
        const kb = std.fmt.parseInt(u64, words.next() orelse return null, 10) catch return null;
        return if (kb == 0) null else kb * 1024;
    }
    return null;
}

/// A v1 limit at or past this is the kernel's "no limit", rounded to a page.
const V1_NO_LIMIT: u64 = 1 << 62;

/// This process's cgroup limit, v2 before v1, or null for none: no cgroup
/// file, or one that says there is no limit.
fn cgroupLimit(root: []const u8) ?Memory {
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    var buf: [4096]u8 = undefined;
    const groups = read(&pb, &buf, root, "/proc/self/cgroup") orelse "";
    // v2: "0::/path". Its own memory.max, else the root's.
    if (groupPath(groups, .v2)) |rel| {
        if (limitIn(root, "/sys/fs/cgroup", rel, "memory.max", .v2)) |b| return .{ .bytes = b, .source = .cgroup_v2 };
    }
    // v1: "N:memory[,…]:/path", under the memory hierarchy.
    if (groupPath(groups, .v1)) |rel| {
        if (limitIn(root, "/sys/fs/cgroup/memory", rel, "memory.limit_in_bytes", .v1)) |b| return .{ .bytes = b, .source = .cgroup_v1 };
    }
    return null;
}

const Version = enum { v1, v2 };

/// This process's cgroup path for `version`, from `/proc/self/cgroup`.
fn groupPath(groups: []const u8, version: Version) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, groups, '\n');
    while (lines.next()) |line| {
        var parts = std.mem.splitScalar(u8, line, ':');
        const id = parts.next() orelse continue;
        const controllers = parts.next() orelse continue;
        const path = parts.rest();
        switch (version) {
            .v2 => if (std.mem.eql(u8, id, "0") and controllers.len == 0) return path,
            .v1 => {
                var cs = std.mem.splitScalar(u8, controllers, ',');
                while (cs.next()) |c| if (std.mem.eql(u8, c, "memory")) return path;
            },
        }
    }
    return null;
}

/// The lowest limit `file` gives in the cgroup `rel` under `mount` and in
/// each cgroup above it: a cgroup's memory is bounded by its ancestors'
/// limits too. Null for none.
fn limitIn(root: []const u8, mount: []const u8, rel: []const u8, file: []const u8, version: Version) ?u64 {
    var low: ?u64 = null;
    var at: []const u8 = std.mem.trim(u8, rel, "/");
    while (true) {
        var sb: [std.fs.max_path_bytes]u8 = undefined;
        const path = (if (at.len > 0)
            std.fmt.bufPrint(&sb, "{s}/{s}/{s}", .{ mount, at, file })
        else
            std.fmt.bufPrint(&sb, "{s}/{s}", .{ mount, file })) catch return low;
        if (limitAt(root, path, version)) |v| low = if (low) |l| @min(l, v) else v;
        if (at.len == 0) return low;
        at = if (std.mem.lastIndexOfScalar(u8, at, '/')) |slash| at[0..slash] else "";
    }
}

/// The limit one cgroup file gives, or null for none: no file, no limit,
/// or text that isn't one.
fn limitAt(root: []const u8, path: []const u8, version: Version) ?u64 {
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    var buf: [64]u8 = undefined;
    const text = std.mem.trim(u8, read(&pb, &buf, root, path) orelse return null, " \t\n");
    if (version == .v2 and std.mem.eql(u8, text, "max")) return null;
    const v = std.fmt.parseInt(u64, text, 10) catch return null;
    if (v == 0 or (version == .v1 and v >= V1_NO_LIMIT)) return null;
    return v;
}

/// `path` under `root`, read into `buf`, or null.
fn read(pb: []u8, buf: []u8, root: []const u8, path: []const u8) ?[]const u8 {
    const full = std.fmt.bufPrint(pb, "{s}{s}", .{ root, path }) catch return null;
    return stdx.fs.readFile(full, buf) catch null;
}

// ─── Tests ─────────────────────────────────────────────────────────────────

const testing = std.testing;

/// A fake filesystem root of fixture files.
const Fixture = struct {
    tmp: testing.TmpDir,
    root_buf: [std.fs.max_path_bytes]u8 = undefined,
    root: []const u8 = "",

    fn init(f: *Fixture) !void {
        f.tmp = testing.tmpDir(.{});
        const abs = try stdx.fs.dirRealpathAlloc(f.tmp.dir, testing.allocator, ".");
        defer testing.allocator.free(abs);
        @memcpy(f.root_buf[0..abs.len], abs);
        f.root = f.root_buf[0..abs.len];
    }

    fn deinit(f: *Fixture) void {
        f.tmp.cleanup();
    }

    fn put(f: *Fixture, path: []const u8, text: []const u8) !void {
        var fb: [std.fs.max_path_bytes]u8 = undefined;
        const full = try std.fmt.bufPrint(&fb, "{s}{s}", .{ f.root, path });
        try stdx.fs.makePath(std.fs.path.dirname(full).?);
        const file = try stdx.fs.createFile(full, .{});
        defer stdx.fs.closeFile(file);
        try stdx.fs.writeAll(file, text);
    }

    fn detect(f: *const Fixture, sysctl: ?u64) Memory {
        return detectAt(f.root, sysctl);
    }
};

const MEMINFO = "MemTotal:       16384000 kB\nMemFree:         1000 kB\n";
const HOST: u64 = 16384000 * 1024;

test "host memory: the host's RAM, with no cgroup limit" {
    var f: Fixture = .{ .tmp = undefined };
    try f.init();
    defer f.deinit();
    try f.put("/proc/meminfo", MEMINFO);
    try testing.expectEqual(Memory{ .bytes = HOST, .source = .meminfo }, f.detect(null));
    // A v2 cgroup with no limit, and a v1 one at the kernel's "none".
    try f.put("/proc/self/cgroup", "0::/\n");
    try f.put("/sys/fs/cgroup/memory.max", "max\n");
    try testing.expectEqual(Memory{ .bytes = HOST, .source = .meminfo }, f.detect(null));
}

test "host memory: a cgroup v2 limit below the host's RAM: the lowest of its own and its ancestors'" {
    var f: Fixture = .{ .tmp = undefined };
    try f.init();
    defer f.deinit();
    try f.put("/proc/meminfo", MEMINFO);
    try f.put("/proc/self/cgroup", "0::/system.slice/flo.service\n");
    try f.put("/sys/fs/cgroup/memory.max", "8589934592\n");
    // No file of its own: an ancestor's binds.
    try testing.expectEqual(Memory{ .bytes = 8 << 30, .source = .cgroup_v2 }, f.detect(null));
    try f.put("/sys/fs/cgroup/system.slice/flo.service/memory.max", "2147483648\n");
    try testing.expectEqual(Memory{ .bytes = 2 << 30, .source = .cgroup_v2 }, f.detect(null));
    // A lower limit on the slice between binds too, and "max" on its own adds none.
    try f.put("/sys/fs/cgroup/system.slice/memory.max", "1073741824\n");
    try f.put("/sys/fs/cgroup/system.slice/flo.service/memory.max", "max\n");
    try testing.expectEqual(Memory{ .bytes = 1 << 30, .source = .cgroup_v2 }, f.detect(null));
}

test "host memory: a cgroup v1 limit, and none at its 'no limit' value" {
    var f: Fixture = .{ .tmp = undefined };
    try f.init();
    defer f.deinit();
    try f.put("/proc/meminfo", MEMINFO);
    try f.put("/proc/self/cgroup", "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n");
    try f.put("/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes", "1073741824\n");
    try testing.expectEqual(Memory{ .bytes = 1 << 30, .source = .cgroup_v1 }, f.detect(null));
    try f.put("/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes", "9223372036854771712\n");
    try testing.expectEqual(Memory{ .bytes = HOST, .source = .meminfo }, f.detect(null));
}

test "host memory: v2 before v1, and a limit above the host's RAM is the host's" {
    var f: Fixture = .{ .tmp = undefined };
    try f.init();
    defer f.deinit();
    try f.put("/proc/meminfo", MEMINFO);
    try f.put("/proc/self/cgroup", "4:memory:/a\n0::/b\n");
    try f.put("/sys/fs/cgroup/memory/a/memory.limit_in_bytes", "1073741824\n");
    try f.put("/sys/fs/cgroup/b/memory.max", "3221225472\n");
    try testing.expectEqual(Memory{ .bytes = 3 << 30, .source = .cgroup_v2 }, f.detect(null));
    try f.put("/sys/fs/cgroup/b/memory.max", "99999999999999\n");
    try testing.expectEqual(Memory{ .bytes = HOST, .source = .meminfo }, f.detect(null));
}

test "host memory: the sysctl's figure with no /proc, and the fallback with nothing" {
    var f: Fixture = .{ .tmp = undefined };
    try f.init();
    defer f.deinit();
    try testing.expectEqual(Memory{ .bytes = 32 << 30, .source = .sysctl }, f.detect(32 << 30));
    try testing.expectEqual(Memory{ .bytes = FALLBACK_BYTES, .source = .fallback }, f.detect(null));
    // Files it can't parse count as none.
    try f.put("/proc/meminfo", "MemTotal: lots\n");
    try f.put("/proc/self/cgroup", "0::/\n");
    try f.put("/sys/fs/cgroup/memory.max", "garbage\n");
    try testing.expectEqual(Memory{ .bytes = FALLBACK_BYTES, .source = .fallback }, f.detect(null));
}

test "host memory: describe names the figure and where it came from" {
    var buf: [96]u8 = undefined;
    try testing.expectEqualStrings("2048 MiB (cgroup v2 memory.max)", (Memory{ .bytes = 2 << 30, .source = .cgroup_v2 }).describe(&buf));
    try testing.expectEqualStrings("8192 MiB (none could be read; assumed)", (Memory{ .bytes = FALLBACK_BYTES, .source = .fallback }).describe(&buf));
}

test "host memory: this machine has a figure" {
    try testing.expect(detect().bytes > 0);
}
