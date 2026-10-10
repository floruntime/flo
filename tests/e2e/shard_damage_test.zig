//! Damage to one shard's data takes that shard offline on this node, not the
//! node: its requests are answered with why, and the other shards serve. A
//! crash mid-write is not damage: the server restarts with every shard up.
//!
//! These assert on real output rather than exit status — the CLI exits 0 even
//! on protocol errors.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

const KEYS = 24;

fn twoShards() !*stdx.testing.TestContext {
    return stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 2, .durability = .sync } });
}

/// Flip a byte in the middle of every sealed segment shard `shard` holds.
fn corruptSegments(data_dir: []const u8, shard: u16) !usize {
    var path_buf: [512]u8 = undefined;
    const segs = try std.fmt.bufPrint(&path_buf, "{s}/{d:0>5}/segs", .{ data_dir, shard });
    var dir = try stdx.fs.openDir(segs, .{ .iterate = true });
    defer stdx.fs.closeDir(dir);
    var corrupted: usize = 0;
    var iter = dir.iterate();
    while (try iter.next(stdx.io.instance())) |de| {
        if (!std.mem.endsWith(u8, de.name, ".flseg")) continue;
        var file_buf: [600]u8 = undefined;
        const path = try std.fmt.bufPrint(&file_buf, "{s}/{s}", .{ segs, de.name });
        const bytes = blk: {
            const f = try stdx.fs.openFile(path, .{});
            defer stdx.fs.closeFile(f);
            break :blk try stdx.fs.readToEndAlloc(f, testing.allocator, 1 << 24);
        };
        defer testing.allocator.free(bytes);
        bytes[bytes.len / 2] ^= 0xff;
        const f = try stdx.fs.createFile(path, .{});
        defer stdx.fs.closeFile(f);
        try stdx.fs.writeAll(f, bytes);
        corrupted += 1;
    }
    return corrupted;
}

test "e2e/damage: a corrupted segment takes its shard offline, and the other shard serves" {
    var ctx = try twoShards();
    defer ctx.deinit();
    var key_buf: [16]u8 = undefined;
    for (0..KEYS) |i| try ctx.exec(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "key-{d}", .{i}), "kept" });

    ctx.server.stop();
    try testing.expect(try corruptSegments(ctx.server.getDataDir(), 1) > 0);
    try ctx.restartServer();

    var served: usize = 0;
    var refused: usize = 0;
    for (0..KEYS) |i| {
        var r = try ctx.cli.run(&.{ "kv", "get", try std.fmt.bufPrint(&key_buf, "key-{d}", .{i}) });
        defer r.deinit();
        if (r.stdoutContains("kept")) {
            served += 1;
        } else if (std.mem.indexOf(u8, r.stderr, "shard 1 is offline on this node: segment") != null or r.stdoutContains("shard 1 is offline on this node: segment")) {
            refused += 1;
        } else {
            std.debug.print("key-{d}: neither served nor refused as offline:\n  stdout: {s}\n  stderr: {s}\n", .{ i, r.stdout, r.stderr });
            return error.TestUnexpectedResult;
        }
    }
    // Keys spread over both shards: shard 0's are served, shard 1's refused.
    try testing.expect(served > 0);
    try testing.expect(refused > 0);
    try testing.expect(try ctx.serverLogsContain("shard 1 is offline on this node"));
}

test "e2e/damage: a server killed mid-writes restarts with every shard serving" {
    var ctx = try twoShards();
    defer ctx.deinit();
    var key_buf: [16]u8 = undefined;
    for (0..KEYS) |i| try ctx.exec(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "key-{d}", .{i}), "kept" });

    // Writes in flight when the process dies.
    const first = try ctx.cli.runAsync(&.{ "kv", "set", "late-0", "maybe" });
    var writers: [8]@TypeOf(first) = undefined;
    writers[0] = first;
    for (writers[1..], 1..) |*w, i| w.* = try ctx.cli.runAsync(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "late-{d}", .{i}), "maybe" });
    ctx.server.sent_kill = true;
    _ = std.c.kill(ctx.server.process.?.id, .KILL);
    for (writers) |w| {
        defer w.deinit();
        var r = w.wait() catch continue;
        r.deinit();
    }
    try ctx.restartServer();

    // Every acked key reads back, on both shards.
    for (0..KEYS) |i| {
        var r = try ctx.cli.run(&.{ "kv", "get", try std.fmt.bufPrint(&key_buf, "key-{d}", .{i}) });
        defer r.deinit();
        if (!r.stdoutContains("kept")) {
            std.debug.print("key-{d} after the crash:\n  stdout: {s}\n  stderr: {s}\n", .{ i, r.stdout, r.stderr });
            return error.TestUnexpectedResult;
        }
    }
    try testing.expect(!try ctx.serverLogsContain("offline on this node"));
}
