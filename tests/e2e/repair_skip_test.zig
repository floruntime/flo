//! A shard halted at an entry is repaired offline on a stopped single node:
//! `flo server repair skip` appends a skip of that entry, and the shard
//! comes up past it. The write never runs, on this build or a later one
//! that could apply it. The halting entry comes from a fault only a
//! `-Dtest-faults` build carries; without one these skip.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const test_faults = @import("src").node.test_faults;
const CliRunner = stdx.testing.CliRunner;

const Halted = struct { shard: usize, index: i64 };

/// The one halted shard in a node's `/status`, or null when none is.
fn haltedShard(port: u16) !?Halted {
    const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", port);
    defer http.deinit();
    var resp = try http.get("/status");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{});
    defer parsed.deinit();
    var found: ?Halted = null;
    for (parsed.value.object.get("shards").?.array.items, 0..) |s, i| {
        if (!std.mem.eql(u8, s.object.get("state").?.string, "halted")) continue;
        if (found != null) return error.MoreThanOneShardHalted;
        found = .{ .shard = i, .index = s.object.get("halt_index").?.integer };
    }
    return found;
}

/// A read of a key that isn't there: exit 1, named not found.
fn expectNotFound(ctx: *stdx.testing.TestContext, key: []const u8) !void {
    var r = try ctx.execResult(&.{ "kv", "get", key });
    defer r.deinit();
    try testing.expectEqual(@as(u8, 1), r.exit_code);
    try testing.expect(std.mem.endsWith(u8, std.mem.trimEnd(u8, r.stderr, "\n"), "[not_found/not_found]"));
}

/// A lone node of two shards, halted by a kv set of the halt key; its
/// shard and index.
fn haltOne(ctx: *stdx.testing.TestContext) !Halted {
    try ctx.exec(&.{ "kv", "set", "before", "kept" });
    {
        var r = try ctx.execResult(&.{ "kv", "set", test_faults.HALT_KEY, "x" });
        defer r.deinit();
    }
    return (try haltedShard(ctx.server.metrics_port)) orelse error.NoShardHalted;
}

fn repairSkip(ctx: *stdx.testing.TestContext, h: Halted, yes: bool) !stdx.testing.CommandResult {
    const offline = try CliRunner.init(testing.allocator, ctx.server.flo_binary, "127.0.0.1:1");
    defer offline.deinit();
    var shard_buf: [8]u8 = undefined;
    var index_buf: [24]u8 = undefined;
    const shard = try std.fmt.bufPrint(&shard_buf, "{d}", .{h.shard});
    const index = try std.fmt.bufPrint(&index_buf, "{d}", .{h.index});
    const base = [_][]const u8{ "server", "repair", "skip", "--config", ctx.server.config_file, "--data-dir", ctx.server.getDataDir(), "--shard", shard, "--index", index, "--reason", "e2e" };
    return if (yes) offline.runRaw(&(base ++ [_][]const u8{"--yes"})) else offline.runRaw(&base);
}

/// Restart as a build with the faults off, which applies the halting entry.
fn startFaultsOff(ctx: *stdx.testing.TestContext) !void {
    ctx.server.extra_env = &.{.{ test_faults.OFF_ENV, "1" }};
    try ctx.restartServer();
}

test "e2e/repair: a halted shard repaired offline comes up past the entry, which never runs" {
    if (!test_faults.enabled) return error.SkipZigTest;
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 2, .durability = .sync, .metrics_enabled = true },
    });
    defer ctx.deinit();
    const h = try haltOne(ctx);
    ctx.server.stop();

    // Without --yes it names the entry and changes nothing.
    {
        var dry = try repairSkip(ctx, h, false);
        defer dry.deinit();
        try testing.expect(dry.stdoutContains("kv_put"));
        try testing.expect(dry.stdoutContains("Run again with --yes"));
    }
    try ctx.restartServer();
    try testing.expectEqual(h.index, ((try haltedShard(ctx.server.metrics_port)) orelse return error.NotHalted).index);
    ctx.server.stop();

    {
        var done = try repairSkip(ctx, h, true);
        defer done.deinit();
        try testing.expect(done.stdoutContains("is skipped"));
    }
    // Twice: the repair holds across restarts.
    for (0..2) |_| {
        try ctx.restartServer();
        try testing.expect((try haltedShard(ctx.server.metrics_port)) == null);
        try expectNotFound(ctx, test_faults.HALT_KEY);
        try testing.expectEqualStrings("kept", try ctx.execCapture(&.{ "kv", "get", "before" }));
    }
    // Writes are taken again, on the repaired shard too.
    try ctx.exec(&.{ "kv", "set", "after", "taken" });
    try testing.expectEqualStrings("taken", try ctx.execCapture(&.{ "kv", "get", "after" }));
}

test "e2e/repair: a later build that could apply a skipped entry still skips it" {
    if (!test_faults.enabled) return error.SkipZigTest;
    // Without the repair, a build with the fault off applies the write.
    {
        var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
            .server = .{ .shards = 2, .durability = .sync, .metrics_enabled = true },
        });
        defer ctx.deinit();
        _ = try haltOne(ctx);
        try startFaultsOff(ctx);
        try testing.expect((try haltedShard(ctx.server.metrics_port)) == null);
        try testing.expectEqualStrings("x", try ctx.execCapture(&.{ "kv", "get", test_faults.HALT_KEY }));
    }
    // With it, that build passes over the entry.
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 2, .durability = .sync, .metrics_enabled = true },
    });
    defer ctx.deinit();
    const h = try haltOne(ctx);
    ctx.server.stop();
    {
        var done = try repairSkip(ctx, h, true);
        defer done.deinit();
        try testing.expect(done.stdoutContains("is skipped"));
    }
    try startFaultsOff(ctx);
    try testing.expect((try haltedShard(ctx.server.metrics_port)) == null);
    try expectNotFound(ctx, test_faults.HALT_KEY);
    try testing.expect(!try ctx.server.logsContain("applied on this node, but the repair"));
}

test "e2e/repair: a running server's data directory is refused" {
    if (!test_faults.enabled) return error.SkipZigTest;
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 2, .durability = .sync, .metrics_enabled = true },
    });
    defer ctx.deinit();
    const h = try haltOne(ctx);
    var refused = try repairSkip(ctx, h, true);
    defer refused.deinit();
    try testing.expect(refused.contains("in use by a running flo server"));
    try testing.expect(!refused.stdoutContains("is skipped"));
    try testing.expectEqual(h.index, ((try haltedShard(ctx.server.metrics_port)) orelse return error.NotHalted).index);
}
