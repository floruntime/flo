//! Two namespaces holding a stream, a queue and a measurement of the same
//! name keep them apart: each is keyed by its name hashed with the
//! namespace's id, from a request and from the log alike, so a read in one
//! never sees the other's records, messages or points, on one shard or
//! many, and after a restart replays the log. The two namespaces are a pair
//! whose names once hashed to the same 32 bits.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const colliding = @import("helpers/colliding_namespaces.zig");

const NAMESPACES = [_][]const u8{ colliding.a, colliding.b };

/// How many records, messages and points namespace `i` writes. The two
/// differ, so a read that merged them, or answered from the wrong one,
/// shows it in a count as well as in the payloads.
fn count(i: usize) usize {
    return i + 2;
}

/// What the CLI prints for `args`, its exit code checked.
fn run(ctx: *stdx.testing.TestContext, args: []const []const u8, want_exit: u8) ![]const u8 {
    var r = try ctx.cli.run(args);
    defer r.deinit();
    if (r.exit_code != want_exit) {
        std.debug.print("{s} {s}: exit {d}, wanted {d}\n  stdout: {s}\n  stderr: {s}\n", .{ args[0], args[1], r.exit_code, want_exit, r.stdout, r.stderr });
        return error.UnexpectedExit;
    }
    return testing.allocator.dupe(u8, std.mem.trimEnd(u8, r.stdout, "\n"));
}

fn parse(comptime T: type, text: []const u8) !std.json.Parsed(T) {
    return std.json.parseFromSlice(T, testing.allocator, text, .{ .ignore_unknown_fields = true });
}

fn payload(buf: []u8, ns: []const u8, k: usize) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}-{d}", .{ ns, k });
}

/// Start a server, write with `write`, check with `verify`, restart, and
/// check again: the log rebuilds each namespace's state apart.
fn isolated(
    shards: u8,
    comptime write: fn (*stdx.testing.TestContext) anyerror!void,
    comptime verify: fn (*stdx.testing.TestContext, restarted: bool) anyerror!void,
) !void {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = shards, .durability = .sync } });
    defer ctx.deinit();
    try write(ctx);
    try verify(ctx, false);
    try ctx.restartServer();
    try verify(ctx, true);
}

// ── Streams ──────────────────────────────────────────────────────────────────

fn writeStreams(ctx: *stdx.testing.TestContext) !void {
    var buf: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        for (1..count(i) + 1) |k| try ctx.exec(&.{ "stream", "append", "same", try payload(&buf, ns, k), "-n", ns });
    }
}

fn verifyStreams(ctx: *stdx.testing.TestContext, _: bool) !void {
    var buf: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        const read = try run(ctx, &.{ "stream", "read", "same", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(read);
        const records = try parse([]const struct { data: []const u8 }, read);
        defer records.deinit();
        try testing.expectEqual(count(i), records.value.len);
        for (records.value, 1..) |r, k| try testing.expectEqualStrings(try payload(&buf, ns, k), r.data);

        const info = try run(ctx, &.{ "stream", "info", "same", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(info);
        const parsed = try parse(struct { namespace: []const u8, message_count: u64 }, info);
        defer parsed.deinit();
        try testing.expectEqualStrings(ns, parsed.value.namespace);
        try testing.expectEqual(count(i), parsed.value.message_count);
    }
}

// ── Queues ───────────────────────────────────────────────────────────────────

const QueueStats = struct { name: []const u8, namespace: []const u8, ready: u64, enqueued: u64, dequeued: u64 };

fn writeQueues(ctx: *stdx.testing.TestContext) !void {
    var buf: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        for (1..count(i) + 1) |k| try ctx.exec(&.{ "queue", "enqueue", "same", try payload(&buf, ns, k), "-n", ns });
    }
}

/// Before the restart each namespace takes its first message; after it, the
/// rest. Either way the stats and what's dequeued are its own.
fn verifyQueues(ctx: *stdx.testing.TestContext, restarted: bool) !void {
    var buf: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        const taken: usize = @intFromBool(restarted);
        const listed = try run(ctx, &.{ "queue", "list", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(listed);
        const stats = try parse([]const QueueStats, listed);
        defer stats.deinit();
        try testing.expectEqual(1, stats.value.len);
        const s = stats.value[0];
        try testing.expectEqualStrings("same", s.name);
        try testing.expectEqualStrings(ns, s.namespace);
        try testing.expectEqual(count(i), s.enqueued);
        try testing.expectEqual(taken, s.dequeued);
        try testing.expectEqual(count(i) - taken, s.ready);

        if (!restarted) {
            const first = try run(ctx, &.{ "queue", "dequeue", "same", "-o", "raw", "-n", ns }, 0);
            defer testing.allocator.free(first);
            try testing.expectEqualStrings(try payload(&buf, ns, 1), first);
            continue;
        }
        const rest = try run(ctx, &.{ "queue", "dequeue", "same", "--count", "16", "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(rest);
        var lines = std.mem.splitScalar(u8, rest, '\n');
        for (2..count(i) + 1) |k| try testing.expectEqualStrings(try payload(&buf, ns, k), lines.next() orelse return error.TooFewMessages);
        try testing.expect(lines.next() == null);
        const empty = try run(ctx, &.{ "queue", "dequeue", "same", "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(empty);
        try testing.expectEqualStrings("(no messages)", empty);
    }
}

// ── Time series ──────────────────────────────────────────────────────────────

fn writeSeries(ctx: *stdx.testing.TestContext) !void {
    var tags: [64]u8 = undefined;
    var ts: [32]u8 = undefined;
    var only: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        const host = try std.fmt.bufPrint(&tags, "host={s}", .{ns});
        for (1..count(i) + 1) |k| {
            try ctx.exec(&.{ "ts", "write", "same", "--tags", host, "--value", "1", "--timestamp", try std.fmt.bufPrint(&ts, "{d}", .{1000 + k}), "-n", ns });
        }
        try ctx.exec(&.{ "ts", "write", try std.fmt.bufPrint(&only, "only_{s}", .{ns}), "--value", "1", "--timestamp", "1000", "-n", ns });
        // The same measurement and tag set in both: each namespace's tag
        // dictionary resolves it apart, so a delete by tag in the first
        // leaves the second's.
        for (1..count(i) + 1) |k| {
            try ctx.exec(&.{ "ts", "write", "shared", "--tags", "site=x", "--value", "1", "--timestamp", try std.fmt.bufPrint(&ts, "{d}", .{1000 + k}), "-n", ns });
        }
    }
    try ctx.exec(&.{ "ts", "delete", "shared", "--tags", "site=x", "--confirm", "-n", NAMESPACES[0] });
}

fn verifySeries(ctx: *stdx.testing.TestContext, _: bool) !void {
    var only: [64]u8 = undefined;
    for (NAMESPACES, 0..) |ns, i| {
        // One series, this namespace's, counting this namespace's points.
        const queried = try run(ctx, &.{ "ts", "query", "same", "--from", "1", "--to", "100000", "--window", "1h", "--agg", "count", "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(queried);
        var want: [32]u8 = undefined;
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "0 {d}.000000", .{count(i)}), queried);

        const tagged = try run(ctx, &.{ "ts", "query", "same", "--tags", try std.fmt.bufPrint(&want, "host={s}", .{ns}), "--from", "1", "--to", "100000", "--window", "1h", "--agg", "count", "-o", "raw", "-n", ns }, 0);
        defer testing.allocator.free(tagged);
        try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "0 {d}.000000", .{count(i)}), tagged);

        // A listing: this namespace's measurements, and no other's; the
        // first namespace's "shared" is deleted, the second's is not.
        const listed = try run(ctx, &.{ "ts", "list", "-o", "json", "-n", ns }, 0);
        defer testing.allocator.free(listed);
        const names = try parse([]const []const u8, listed);
        defer names.deinit();
        const mine = try std.fmt.bufPrint(&only, "only_{s}", .{ns});
        var want_names: [3][]const u8 = .{ mine, "same", "shared" };
        const n_want: usize = if (i == 0) 2 else 3;
        var got: [3][]const u8 = undefined;
        try testing.expectEqual(n_want, names.value.len);
        @memcpy(got[0..n_want], names.value);
        std.mem.sort([]const u8, got[0..n_want], {}, lessThanStr);
        std.mem.sort([]const u8, want_names[0..n_want], {}, lessThanStr);
        for (want_names[0..n_want], got[0..n_want]) |w, g| try testing.expectEqualStrings(w, g);

        if (i == 1) {
            const shared = try run(ctx, &.{ "ts", "query", "shared", "--tags", "site=x", "--from", "1", "--to", "100000", "--window", "1h", "--agg", "count", "-o", "raw", "-n", ns }, 0);
            defer testing.allocator.free(shared);
            try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "0 {d}.000000", .{count(i)}), shared);
        }
    }
}

fn lessThanStr(_: void, x: []const u8, y: []const u8) bool {
    return std.mem.lessThan(u8, x, y);
}

test "e2e/namespace/isolation: the pair the tests use once collided on the 32-bit hash" {
    const old = struct {
        fn hash(name: []const u8) u32 {
            return @truncate(std.hash.Wyhash.hash(0, name));
        }
    }.hash;
    try testing.expect(!std.mem.eql(u8, colliding.a, colliding.b));
    try testing.expectEqual(old(colliding.a), old(colliding.b));
}

test "e2e/stream/isolation: a stream named in two namespaces reads and reports only its own records, on 1 shard and after a restart" {
    try isolated(1, writeStreams, verifyStreams);
}

test "e2e/stream/isolation: a stream named in two namespaces reads and reports only its own records, on 4 shards and after a restart" {
    try isolated(4, writeStreams, verifyStreams);
}

test "e2e/queue/isolation: a queue named in two namespaces dequeues and counts only its own messages, on 1 shard and after a restart" {
    try isolated(1, writeQueues, verifyQueues);
}

test "e2e/queue/isolation: a queue named in two namespaces dequeues and counts only its own messages, on 4 shards and after a restart" {
    try isolated(4, writeQueues, verifyQueues);
}

test "e2e/ts/isolation: a measurement named in two namespaces queries and lists only its own points, on 1 shard and after a restart" {
    try isolated(1, writeSeries, verifySeries);
}

test "e2e/ts/isolation: a measurement named in two namespaces queries and lists only its own points, on 4 shards and after a restart" {
    try isolated(4, writeSeries, verifySeries);
}
