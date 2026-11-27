//! `flo workflow runs-delete` and `flo action runs-delete`: an owner's runs
//! that ended before a time deleted, a batch per request, asking again
//! while the server says more ended before it.

const std = @import("std");
const commander = @import("../commander/mod.zig");
const outcome = @import("../outcome.zig");
const client_mod = @import("../client/mod.zig");
const cli_config = @import("../config.zig");
const schema = @import("../../protocol/schema.zig");
const proto = @import("../../protocol/proto.zig");
const action_ops = @import("../../protocol/action_ops.zig");

const Client = client_mod.Client;

/// The command, for an owner named by positional `key`.
pub fn builder(allocator: std.mem.Allocator, comptime key: []const u8, comptime about: []const u8, comptime example: []const u8, handler: commander.RunFn) *commander.Builder {
    return commander.newBuilder(allocator)
        .name("runs-delete")
        .about(about)
        .arg(key, "Whose ended runs to delete")
        .stringFlag("older-than", 0, "", "Delete runs that ended longer ago than this: a number and s, m, h or d (30d)")
        .stringFlag("ended-before", 0, "", "Delete runs that ended before this, in Unix milliseconds")
        .examples(&.{example})
        .action(handler);
}

/// The bound a run's end must be before, in Unix ms, from the flags; null
/// when neither or both were given, or one doesn't parse.
fn boundOf(ctx: *commander.Context, now_ms: i64) ?i64 {
    const older = ctx.getString("older-than") orelse "";
    const before = ctx.getString("ended-before") orelse "";
    if ((older.len == 0) == (before.len == 0)) return null;
    if (before.len > 0) return std.fmt.parseInt(i64, before, 10) catch null;
    return now_ms -| (durationMs(older) orelse return null);
}

/// `30d`, `12h`, `90m`, `45s` in ms; null for anything else.
pub fn durationMs(s: []const u8) ?i64 {
    if (s.len < 2) return null;
    const n = std.fmt.parseInt(i64, s[0 .. s.len - 1], 10) catch return null;
    if (n < 0) return null;
    const unit: i64 = switch (s[s.len - 1]) {
        's' => 1000,
        'm' => 60 * 1000,
        'h' => 60 * 60 * 1000,
        'd' => 24 * 60 * 60 * 1000,
        else => return null,
    };
    return std.math.mul(i64, n, unit) catch null;
}

/// Sends `op` for the owner `key` names until no more ended before the
/// bound, or a request deleted none (another delete took them meanwhile).
pub fn run(ctx: *commander.Context, comptime op: proto.OpCode, comptime key: []const u8) commander.Error!void {
    const owner = ctx.getPositional(key) orelse return outcome.usage(ctx, key ++ " is required", .{});
    const before_ms = boundOf(ctx, @import("stdx").time.milliTimestamp()) orelse
        return outcome.usage(ctx, "give exactly one of --older-than (30d) or --ended-before (Unix ms)", .{});
    const namespace = cli_config.getNamespace(ctx);
    var client = Client.init(ctx.allocator, cli_config.getEndpoint(ctx));
    defer client.deinit();
    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var deleted: u64 = 0;
    var left: u32 = 0;
    while (true) {
        var body: [action_ops.RunsDeleteRequest.max_size]u8 = undefined;
        const value = action_ops.RunsDeleteRequest.encodeInto(&body, .{ .ended_before_ms = before_ms, .limit = action_ops.MAX_RUNS_DELETE }) catch return outcome.malformed(ctx, "request");
        var result = client.sendRequest(op, namespace, owner, value) catch |err| return outcome.requestFailed(ctx, err);
        defer result.deinit();
        try outcome.check(ctx, result);
        const data = result.asRawData() orelse return outcome.malformed(ctx, "runs-delete response (empty)");
        var diag: schema.Diagnostic = .{};
        const r = action_ops.RunsDeleteResponse.decodeAll(data, &diag) catch return outcome.malformed(ctx, "runs-delete response");
        deleted += r.deleted;
        left = r.orphans_left;
        if (!r.more or r.deleted == 0) break;
    }
    ctx.print("Deleted {d} ended run(s) of {s}\n", .{ deleted, owner });
    if (left > 0) ctx.print("{d} orphaned histories are left; run it again to drop them\n", .{left});
}

test "runs-delete: a duration's units" {
    try std.testing.expectEqual(@as(?i64, 30 * 24 * 60 * 60 * 1000), durationMs("30d"));
    try std.testing.expectEqual(@as(?i64, 90 * 60 * 1000), durationMs("90m"));
    try std.testing.expectEqual(@as(?i64, 45_000), durationMs("45s"));
    try std.testing.expectEqual(@as(?i64, null), durationMs("30"));
    try std.testing.expectEqual(@as(?i64, null), durationMs("-1d"));
    try std.testing.expectEqual(@as(?i64, null), durationMs("1w"));
}
