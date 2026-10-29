//! A shard offline on a stopped single node because a segment won't read is
//! cut back to the entries before it: `flo server salvage` drops the rest,
//! and the shard comes up with what it kept.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const CliRunner = stdx.testing.CliRunner;

/// Shard 0's state in a node's `/status`.
fn shardState(port: u16, buf: []u8) ![]const u8 {
    const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", port);
    defer http.deinit();
    var resp = try http.get("/status");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{});
    defer parsed.deinit();
    const state = parsed.value.object.get("shards").?.array.items[0].object.get("state").?.string;
    return std.fmt.bufPrint(buf, "{s}", .{state});
}

fn salvage(ctx: *stdx.testing.TestContext, after: []const u8, yes: bool) !stdx.testing.CommandResult {
    const offline = try CliRunner.init(testing.allocator, ctx.server.flo_binary, "127.0.0.1:1");
    defer offline.deinit();
    const base = [_][]const u8{ "server", "salvage", "--config", ctx.server.config_file, "--data-dir", ctx.server.getDataDir(), "--shard", "0", "--truncate-after", after };
    var args: [base.len + 1][]const u8 = undefined;
    @memcpy(args[0..base.len], &base);
    args[base.len] = "--yes";
    return offline.runRaw(if (yes) &args else &base);
}

/// The sorted segment files of shard 0.
fn segments(ctx: *stdx.testing.TestContext, out: *std.ArrayListUnmanaged([]const u8)) ![]const u8 {
    const segs = try std.fmt.allocPrint(testing.allocator, "{s}/00000/segs", .{ctx.server.getDataDir()});
    var dir = try stdx.fs.openDir(segs, .{ .iterate = true });
    defer stdx.fs.closeDir(dir);
    var iter = dir.iterate();
    while (try iter.next(stdx.io.instance())) |de| {
        if (std.mem.endsWith(u8, de.name, ".flseg")) try out.append(testing.allocator, try testing.allocator.dupe(u8, de.name));
    }
    std.mem.sort([]const u8, out.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    return segs;
}

test "e2e/salvage: a shard offline on a segment that won't read comes up with the entries before it" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 1, .durability = .sync, .metrics_enabled = true },
    });
    defer ctx.deinit();
    for ([_][]const u8{ "kept1", "kept2", "lost1", "lost2" }) |k| try ctx.exec(&.{ "kv", "set", k, "v" });
    ctx.server.stop();

    // Every sync write seals its own segment: the last two hold lost1 and
    // lost2. Spoil the first one's header.
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (names.items) |n| testing.allocator.free(n);
        names.deinit(testing.allocator);
    }
    const segs = try segments(ctx, &names);
    defer testing.allocator.free(segs);
    const damaged = names.items[names.items.len - 2];
    const path = try std.fmt.allocPrint(testing.allocator, "{s}/{s}", .{ segs, damaged });
    defer testing.allocator.free(path);
    {
        const bytes = try stdx.fs.readFileAlloc(testing.allocator, path, 1 << 20);
        defer testing.allocator.free(bytes);
        @memcpy(bytes[0..4], "XXXX");
        const file = try stdx.fs.createFile(path, .{});
        defer stdx.fs.closeFile(file);
        try stdx.fs.writeAll(file, bytes);
    }
    const first_lost = try std.fmt.parseInt(u64, damaged[0 .. damaged.len - ".flseg".len], 10);
    var keep_buf: [24]u8 = undefined;
    const keep = try std.fmt.bufPrint(&keep_buf, "{d}", .{first_lost - 1});
    var past_buf: [24]u8 = undefined;
    const past = try std.fmt.bufPrint(&past_buf, "{d}", .{first_lost});

    try ctx.restartServer();
    var state_buf: [32]u8 = undefined;
    try testing.expectEqualStrings("offline", try shardState(ctx.server.metrics_port, &state_buf));
    // Refused while the server runs.
    {
        var refused = try salvage(ctx, keep, true);
        defer refused.deinit();
        try testing.expect(refused.contains("in use by a running flo server"));
    }
    ctx.server.stop();

    // At the damage: refused, naming the highest entry it can keep.
    {
        var refused = try salvage(ctx, past, false);
        defer refused.deinit();
        try testing.expect(refused.exit_code != 0);
        const want = try std.fmt.allocPrint(testing.allocator, "the highest entry shard 0 can keep is {s}", .{keep});
        defer testing.allocator.free(want);
        try testing.expect(refused.contains(want));
    }
    // Without --yes it warns and changes nothing.
    {
        var dry = try salvage(ctx, keep, false);
        defer dry.deinit();
        try testing.expectEqual(@as(u8, 0), dry.exit_code);
        try testing.expect(dry.stdoutContains("WARNING: this drops at least 2 entries"));
        try testing.expect(dry.stdoutContains("told succeeded"));
        try testing.expect(dry.stdoutContains("Run again with --yes"));
    }
    stdx.fs.closeFile(try stdx.fs.openFile(path, .{}));
    {
        var done = try salvage(ctx, keep, true);
        defer done.deinit();
        try testing.expectEqual(@as(u8, 0), done.exit_code);
        try testing.expect(done.stdoutContains("now ends at entry"));
    }

    // Twice: the cut holds across restarts.
    for (0..2) |_| {
        try ctx.restartServer();
        try testing.expectEqualStrings("ok", try shardState(ctx.server.metrics_port, &state_buf));
        try testing.expectEqualStrings("v", try ctx.execCapture(&.{ "kv", "get", "kept2" }));
        var r = try ctx.execResult(&.{ "kv", "get", "lost1" });
        defer r.deinit();
        try testing.expectEqual(@as(u8, 1), r.exit_code);
        try testing.expect(std.mem.endsWith(u8, std.mem.trimEnd(u8, r.stderr, "\n"), "[not_found/not_found]"));
    }
    try ctx.exec(&.{ "kv", "set", "after", "taken" });
    try testing.expectEqualStrings("taken", try ctx.execCapture(&.{ "kv", "get", "after" }));
}

test "e2e/salvage: a cut that drops a repair of an entry it keeps is refused unless told to undo it" {
    const test_faults = @import("src").node.test_faults;
    if (!test_faults.enabled) return error.SkipZigTest;
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 1, .durability = .sync, .metrics_enabled = true },
    });
    defer ctx.deinit();
    {
        var r = try ctx.execResult(&.{ "kv", "set", test_faults.HALT_KEY, "x" });
        defer r.deinit();
    }
    var state_buf: [32]u8 = undefined;
    try testing.expectEqualStrings("halted", try shardState(ctx.server.metrics_port, &state_buf));
    const halt_index = blk: {
        const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", ctx.server.metrics_port);
        defer http.deinit();
        var resp = try http.get("/status");
        defer resp.deinit();
        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{});
        defer parsed.deinit();
        break :blk parsed.value.object.get("shards").?.array.items[0].object.get("halt_index").?.integer;
    };
    ctx.server.stop();
    var index_buf: [24]u8 = undefined;
    const index = try std.fmt.bufPrint(&index_buf, "{d}", .{halt_index});
    {
        const offline = try CliRunner.init(testing.allocator, ctx.server.flo_binary, "127.0.0.1:1");
        defer offline.deinit();
        var done = try offline.runRaw(&.{ "server", "repair", "skip", "--config", ctx.server.config_file, "--data-dir", ctx.server.getDataDir(), "--shard", "0", "--index", index, "--yes" });
        defer done.deinit();
        try testing.expect(done.stdoutContains("is skipped"));
    }
    try ctx.restartServer();
    try ctx.exec(&.{ "kv", "set", "after", "taken" });
    ctx.server.stop();

    // Kept: the halting entry. Dropped: the repair, and the write after it.
    {
        var refused = try salvage(ctx, index, true);
        defer refused.deinit();
        try testing.expect(refused.exit_code != 0);
        try testing.expect(refused.stdoutContains("drops the repair at entry"));
        try testing.expect(refused.contains("--undo-repairs"));
    }
    try ctx.restartServer();
    try testing.expectEqualStrings("ok", try shardState(ctx.server.metrics_port, &state_buf));
    ctx.server.stop();
    {
        const offline = try CliRunner.init(testing.allocator, ctx.server.flo_binary, "127.0.0.1:1");
        defer offline.deinit();
        var done = try offline.runRaw(&.{ "server", "salvage", "--config", ctx.server.config_file, "--data-dir", ctx.server.getDataDir(), "--shard", "0", "--truncate-after", index, "--undo-repairs", "--yes" });
        defer done.deinit();
        try testing.expectEqual(@as(u8, 0), done.exit_code);
    }
    // The repair is gone, so the entry halts the shard again.
    try ctx.restartServer();
    try testing.expectEqualStrings("halted", try shardState(ctx.server.metrics_port, &state_buf));
}
