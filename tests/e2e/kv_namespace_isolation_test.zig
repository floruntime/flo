//! Namespaces holding the same keys keep them apart: every key is stored
//! behind its namespace's id, so a read, a listing or a transaction in one
//! never reaches the other's, on one shard or many, and after a restart
//! replays the log. Two of the namespaces are a pair whose names once hashed
//! to the same 32 bits.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const colliding = @import("helpers/colliding_namespaces.zig");

const NAMESPACES = [_][]const u8{ "default", colliding.a, colliding.b };

/// The namespace after `i`, whose own key namespace `i` must not see.
fn other(i: usize) []const u8 {
    return NAMESPACES[(i + 1) % NAMESPACES.len];
}

/// What the CLI prints for `args`, its exit code checked.
fn run(ctx: *stdx.testing.TestContext, args: []const []const u8, want_exit: u8) ![]const u8 {
    var r = try ctx.cli.run(args);
    defer r.deinit();
    if (r.exit_code != want_exit) {
        std.debug.print("{s}: exit {d}, wanted {d}\n  stdout: {s}\n  stderr: {s}\n", .{ args[1], r.exit_code, want_exit, r.stdout, r.stderr });
        return error.UnexpectedExit;
    }
    return testing.allocator.dupe(u8, std.mem.trimEnd(u8, r.stdout, "\n"));
}

/// Write the same keys in each namespace, values naming the namespace: two
/// plain sets of "{t}same", then a transaction setting it again with a key
/// only transactions write.
fn write(ctx: *stdx.testing.TestContext) !void {
    var b1: [64]u8 = undefined;
    var b2: [64]u8 = undefined;
    for (NAMESPACES) |ns| {
        for ([_][]const u8{ "v1", "v2" }) |v| try ctx.exec(&.{ "kv", "set", "{t}same", try std.fmt.bufPrint(&b1, "{s}-{s}", .{ ns, v }), "-n", ns });
        try ctx.exec(&.{ "kv", "set", "two", try std.fmt.bufPrint(&b1, "{s}-two", .{ns}), "-n", ns });
        try ctx.exec(&.{ "kv", "set", try std.fmt.bufPrint(&b2, "only-{s}", .{ns}), "here", "-n", ns });

        const begun = try run(ctx, &.{ "kv", "txn", "begin", "{t}", "-n", ns }, 0);
        defer testing.allocator.free(begun);
        const at = (std.mem.indexOf(u8, begun, "txn: ") orelse return error.MissingTxnId) + "txn: ".len;
        var end = at;
        while (end < begun.len and std.ascii.isDigit(begun[end])) end += 1;
        const txn = begun[at..end];
        try ctx.exec(&.{ "kv", "set", "{t}same", try std.fmt.bufPrint(&b1, "{s}-v3", .{ns}), "--txn", txn, "-n", ns });
        try ctx.exec(&.{ "kv", "set", "{t}txn", try std.fmt.bufPrint(&b1, "{s}-txn", .{ns}), "--txn", txn, "-n", ns });
        const committed = try run(ctx, &.{ "kv", "txn", "commit", txn, "-n", ns }, 0);
        defer testing.allocator.free(committed);
        try testing.expect(std.mem.endsWith(u8, committed, "ops: 2"));
    }
}

/// Each namespace reads back only its own values, through every read.
fn verify(ctx: *stdx.testing.TestContext) !void {
    var want: [128]u8 = undefined;
    var key: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        for ([_][2][]const u8{ .{ "{t}same", "v3" }, .{ "two", "two" }, .{ "{t}txn", "txn" } }) |kv| {
            const got = try run(ctx, &.{ "kv", "get", kv[0], "-o", "raw", "-n", ns }, 0);
            defer testing.allocator.free(got);
            try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "{s}-{s}", .{ ns, kv[1] }), got);
        }
        // No other namespace's own key is here.
        for (NAMESPACES) |them| {
            if (std.mem.eql(u8, them, ns)) continue;
            const missing = try run(ctx, &.{ "kv", "get", try std.fmt.bufPrint(&key, "only-{s}", .{them}), "-n", ns }, 1);
            testing.allocator.free(missing);
        }
        const theirs = try std.fmt.bufPrint(&key, "only-{s}", .{other(i)});

        // A listing: this namespace's keys, as named, and no other's.
        const listed = try run(ctx, &.{ "kv", "scan", "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(listed);
        var names: [8][]const u8 = undefined;
        var n: usize = 0;
        var lines = std.mem.tokenizeScalar(u8, listed, '\n');
        while (lines.next()) |line| : (n += 1) names[n] = line;
        std.mem.sort([]const u8, names[0..n], {}, struct {
            fn less(_: void, x: []const u8, y: []const u8) bool {
                return std.mem.lessThan(u8, x, y);
            }
        }.less);
        var mine_buf: [64]u8 = undefined;
        const mine = try std.fmt.bufPrint(&mine_buf, "only-{s}", .{ns});
        const expected = [_][]const u8{ mine, "two", "{t}same", "{t}txn" };
        try testing.expectEqual(expected.len, n);
        for (expected, names[0..n]) |w, g| try testing.expectEqualStrings(w, g);

        // Many keys at once: each from this namespace, the other's absent.
        const many = try run(ctx, &.{ "kv", "mget", "{t}same", "two", "{t}txn", theirs, "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(many);
        var rows = std.mem.splitScalar(u8, many, '\n');
        for ([_][]const u8{ "v3", "two", "txn" }, [_][]const u8{ "{t}same", "two", "{t}txn" }) |suffix, k| {
            const row = rows.next() orelse return error.TooFewRows;
            try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "{s}\t{s}-{s}", .{ k, ns, suffix }), row);
        }
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "{s}\t(nil)", .{theirs}), rows.next() orelse return error.TooFewRows);

        // A key's versions: this namespace's three, newest first.
        const versions = try run(ctx, &.{ "kv", "history", "{t}same", "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(versions);
        var vlines = std.mem.tokenizeScalar(u8, versions, '\n');
        for ([_][]const u8{ "v3", "v2", "v1" }) |v| {
            const line = vlines.next() orelse return error.TooFewVersions;
            try testing.expect(std.mem.endsWith(u8, line, try std.fmt.bufPrint(&want, "\t{s}-{s}", .{ ns, v })));
        }
        try testing.expect(vlines.next() == null);
    }
}

fn isolated(shards: u8) !void {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = shards, .durability = .sync } });
    defer ctx.deinit();
    try write(ctx);
    try verify(ctx);
    // A restart rebuilds every key from the log: they come back apart.
    try ctx.restartServer();
    try verify(ctx);
}

test "e2e/kv: three namespaces with the same keys keep them apart through set, get, scan, mget, history and a transaction, on 1 shard and after a restart" {
    try isolated(1);
}

test "e2e/kv: three namespaces with the same keys keep them apart through set, get, scan, mget, history and a transaction, on 4 shards and after a restart" {
    try isolated(4);
}
