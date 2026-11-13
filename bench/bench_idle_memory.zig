//! Benchmark: the memory an idle shard holds. Brings up SHARDS shards, each
//! with a data directory under /tmp and the default ring sizes, and prints
//! the resident memory they add, per shard. Benches
//! build ReleaseSafe, as a release does: its allocation fill is what made
//! every allocated page resident.

const std = @import("std");
const builtin = @import("builtin");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");

const Shard = src.node.shard.Shard;
const Partition = src.storage.partition.Partition;

const SHARDS = 8;

/// Resident memory in KiB: on Linux what is resident now (`/proc/self/statm`),
/// so what init allocated and freed again isn't counted; elsewhere the peak
/// so far, which init only raises.
fn residentKiB() u64 {
    if (builtin.os.tag == .linux) {
        var buf: [128]u8 = undefined;
        const text = stdx.fs.readFile("/proc/self/statm", &buf) catch return 0;
        var it = std.mem.tokenizeScalar(u8, text, ' ');
        _ = it.next();
        const pages = std.fmt.parseInt(u64, it.next() orelse return 0, 10) catch return 0;
        return pages * std.heap.pageSize() / 1024;
    }
    var ru: std.c.rusage = undefined;
    if (std.c.getrusage(std.c.rusage.SELF, &ru) != 0) return 0;
    const max: u64 = @intCast(ru.maxrss);
    // macOS reports bytes.
    return if (builtin.os.tag == .macos) max / 1024 else max;
}

pub fn main() !void {
    mode.print();
    const allocator = std.heap.c_allocator;
    var dir_buf: [64]u8 = undefined;
    const root = try std.fmt.bufPrint(&dir_buf, "/tmp/flo-bench-idle-{d}", .{std.c.getpid()});
    try stdx.fs.makePath(root);
    defer stdx.fs.deleteTree(root) catch {};

    const pipe_fds = try stdx.io.pipe();
    defer _ = std.c.close(pipe_fds[1]);
    defer _ = std.c.close(pipe_fds[0]);

    const before = residentKiB();
    var shards: [SHARDS]Shard = undefined;
    var dirs: [SHARDS][96]u8 = undefined;
    for (&shards, 0..) |*s, i| {
        const dir = try std.fmt.bufPrint(&dirs[i], "{s}/n{d}", .{ root, i });
        try stdx.fs.makePath(dir);
        s.* = try Shard.init(allocator, 0, 1, 4096, pipe_fds[0], dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{}, .{});
    }
    const after = residentKiB();
    for (&shards) |*s| s.deinit();
    std.debug.print("  {d} idle shards, default ring sizes: {d} KiB resident added, {d} KiB per shard\n", .{ SHARDS, after - before, (after - before) / SHARDS });
}
