//! Benchmark: the memory an idle shard holds. Brings up SHARDS shards, each
//! with a data directory under /tmp and the default ring sizes, and prints
//! the process's peak resident memory before and after, per shard. Benches
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

/// Peak resident memory so far, in KiB: it only grows, and init only adds.
fn peakKiB() u64 {
    var ru: std.c.rusage = undefined;
    if (std.c.getrusage(std.c.rusage.SELF, &ru) != 0) return 0;
    const max: u64 = @intCast(ru.maxrss);
    // Linux reports KiB, macOS bytes.
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

    const before = peakKiB();
    var shards: [SHARDS]Shard = undefined;
    var dirs: [SHARDS][96]u8 = undefined;
    for (&shards, 0..) |*s, i| {
        const dir = try std.fmt.bufPrint(&dirs[i], "{s}/n{d}", .{ root, i });
        try stdx.fs.makePath(dir);
        s.* = try Shard.init(allocator, 0, 1, 4096, pipe_fds[0], dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{}, .{});
    }
    const after = peakKiB();
    for (&shards) |*s| s.deinit();
    std.debug.print("  {d} idle shards, default ring sizes: {d} KiB resident added, {d} KiB per shard\n", .{ SHARDS, after - before, (after - before) / SHARDS });
}
