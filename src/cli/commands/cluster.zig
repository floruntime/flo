//! Cluster management commands for Flo CLI
//!
//! Usage:
//!   flo cluster status [--endpoint <host:port>]     Show cluster health
//!   flo cluster members [--endpoint <host:port>]    List cluster members
//!   flo cluster promote <id>...                     Make caught-up replicas voters
//!   flo cluster remove <id> [--yes]                 Remove a member

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const proto = @import("../../protocol/proto.zig");
const output = @import("../output.zig");
const cli_config = @import("../config.zig");
const outcome = @import("../outcome.zig");
const client_mod = @import("../client/mod.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the cluster command tree
pub fn createClusterCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("cluster")
        .about("Cluster management commands")
        .group("Cluster Commands")
        .longAbout(
            \\Manage and monitor the Flo cluster.
            \\
            \\These commands interact with the distributed cluster, querying
            \\any node which will forward requests to the leader as needed.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("status")
                .about("Show cluster status and health")
                .examples(&.{
                    "flo cluster status",
                    "flo cluster status --endpoint localhost:9000",
                    "flo cluster status -o json",
                })
                .action(wrapHandler(runStatus)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("members")
                .about("List all cluster members")
                .examples(&.{
                    "flo cluster members",
                    "flo cluster members --endpoint localhost:9000",
                    "flo cluster members -o json",
                })
                .action(wrapHandler(runMembers)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("promote")
                .about("Make caught-up replicas voters, one at a time")
                .longAbout(
                    \\A node joins as a replica: it takes the log but does not vote. The
                    \\leader makes up to three voters on its own; past that, promote a
                    \\replica here. Odd numbers of voters tolerate more failures.
                )
                .variadicArg("ids", "Ids of the replicas to promote")
                .examples(&.{
                    "flo cluster promote 4",
                    "flo cluster promote 4 5",
                })
                .action(wrapHandler(runPromote)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("remove")
                .about("Remove a member from the cluster")
                .longAbout(
                    \\The node stops taking part and cannot rejoin under its id. Removing
                    \\the leader works: it hands over after the change commits. A
                    \\removal that leaves an even number of voters, or two, needs --yes.
                )
                .arg("id", "Id of the member to remove")
                .boolFlag("yes", 0, "Remove even when it leaves an even number of voters, or two")
                .examples(&.{
                    "flo cluster remove 3",
                    "flo cluster remove 3 --yes",
                })
                .action(wrapHandler(runRemove)),
        )
        .build();
}

/// Format a node ID as a human-readable name.
/// Generates "flo-XXXXXX" format using lower 24 bits of node_id.
fn formatNodeId(buf: *[11]u8, node_id: u32) []const u8 {
    // Use lower 24 bits for 6 hex chars (16.7M combinations)
    const short_hash: u24 = @truncate(node_id);
    _ = std.fmt.bufPrint(buf, "flo-{x:0>6}", .{short_hash}) catch "flo-000000";
    return buf[0..10];
}

/// Sends one request to the endpoint and returns its ok answer; the caller
/// frees it.
fn ask(ctx: *commander.Context, op: proto.OpCode, value: []const u8) commander.Error!client_mod.Response {
    const endpoint = cli_config.getEndpoint(ctx);
    var client = client_mod.Client.init(ctx.allocator, endpoint);
    defer client.deinit();
    client.connect() catch |err| return outcome.connectFailed(ctx, err, endpoint);
    var response = client.sendRequest(op, "", "", value) catch |err| return outcome.requestFailed(ctx, err);
    outcome.check(ctx, response) catch |err| {
        response.deinit();
        return err;
    };
    return response;
}

fn runStatus(ctx: *commander.Context) commander.Error!void {
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    var response = try ask(ctx, .cluster_status, "");
    defer response.deinit();

    // Parse response data
    // Expected format: [node_id: u32][leader_id: u32][term: u64][state: u8][member_count: u32]
    if (response.data.len < 21) return outcome.malformed(ctx, "cluster status");

    const node_id = std.mem.readInt(u32, response.data[0..4], .little);
    const leader_id = std.mem.readInt(u32, response.data[4..8], .little);
    const term = std.mem.readInt(u64, response.data[8..16], .little);
    const state = response.data[16];
    const member_count = std.mem.readInt(u32, response.data[17..21], .little);

    // Generate human-readable node names
    var node_name_buf: [11]u8 = undefined;
    var leader_name_buf: [11]u8 = undefined;
    const node_name = formatNodeId(&node_name_buf, node_id);
    const leader_name = formatNodeId(&leader_name_buf, leader_id);

    const role_str = switch (state) {
        0 => "follower",
        1 => "electing",
        2 => "leader",
        3 => "joining",
        4 => "diverged",
        5 => "guarded: catching up",
        6 => "guarded: confirming the term",
        7 => "replica",
        8 => "removed",
        else => "unknown",
    };
    // No leader is known while electing or joining.
    const leader_shown = if (leader_id == 0) "none" else leader_name;
    const leader_json = if (leader_id == 0) "null" else leader_name;

    if (json_output) {
        ctx.print("{{\"node_id\":\"{s}\",\"address\":\"{s}\",\"leader_id\":{s}{s}{s},\"term\":{d},\"role\":\"{s}\",\"members\":{d}}}\n", .{
            node_name,
            endpoint,
            if (leader_id == 0) "" else "\"",
            leader_json,
            if (leader_id == 0) "" else "\"",
            term,
            role_str,
            member_count,
        });
    } else {
        ctx.print("\nCluster Status\n", .{});
        ctx.print("──────────────\n", .{});
        ctx.print("Node ID:    {s}\n", .{node_name});
        ctx.print("Address:    {s}\n", .{endpoint});
        ctx.print("Role:       {s}\n", .{role_str});
        ctx.print("Leader:     {s}\n", .{leader_shown});
        ctx.print("Term:       {d}\n", .{term});
        ctx.print("Members:    {d}\n", .{member_count});
        if (state == 5 or state == 6) ctx.print("\nThis node has no log or no hard state of its own (new, or lost). It votes only once a leader has caught\nit up and the members have confirmed the term. If a majority lost their data, see flo server inspect / force-members.\n", .{});
        if (state == 4) ctx.print("\nThis node's data diverged from the group and it takes no writes.\nStop it, move its data directory aside, and start it again with --join <a live member>.\nIt rejoins with no log and votes only once it has caught up.\n", .{});
        ctx.print("\n", .{});
    }
}

fn runMembers(ctx: *commander.Context) commander.Error!void {
    const json_output = output.getFormat(ctx) == .json;

    var response = try ask(ctx, .cluster_members, "");
    defer response.deinit();

    // [leader:u32][commit:u64][members:u8], per member
    // [id:u32][role:u8][ip4:4][port:u16][contact_ago_ms:u32][match:u64],
    // then [removed:u8], per removed id [id:u32][when_ms:u64].
    const data = response.data;
    if (data.len < 13) {
        return outcome.malformed(ctx, "member list");
    }
    const leader_id = std.mem.readInt(u32, data[0..4], .little);
    const commit = std.mem.readInt(u64, data[4..12], .little);
    const count = data[12];
    if (data.len < 13 + @as(usize, count) * 23 + 1) {
        return outcome.malformed(ctx, "member list");
    }
    const removed_count = data[13 + @as(usize, count) * 23];
    if (data.len != 14 + @as(usize, count) * 23 + @as(usize, removed_count) * 12) {
        return outcome.malformed(ctx, "member list");
    }

    var table = output.Table.init(ctx.allocator);
    defer table.deinit();
    if (!json_output) {
        for ([_][]const u8{ "ID", "ROLE", "ADDRESS", "LAST CONTACT", "LAG" }) |col| table.addColumn(col, .left) catch return error.OutOfMemory;
    } else {
        ctx.print("{{\"leader\":{d},\"members\":[", .{leader_id});
    }
    var cells: [5][32]u8 = undefined;
    for (0..count) |i| {
        const m = data[13 + i * 23 ..][0..23];
        const id = std.mem.readInt(u32, m[0..4], .little);
        const role: []const u8 = switch (m[4]) {
            0 => if (id == leader_id) "voter (leader)" else "voter",
            1 => "replica",
            2 => "joining",
            else => "unknown",
        };
        const port = std.mem.readInt(u16, m[9..11], .little);
        const contact = std.mem.readInt(u32, m[11..15], .little);
        const match = std.mem.readInt(u64, m[15..23], .little);
        const address = if (port != 0) std.fmt.bufPrint(&cells[0], "{d}.{d}.{d}.{d}:{d}", .{ m[5], m[6], m[7], m[8], port }) catch "?" else "-";
        const contact_s = if (contact == std.math.maxInt(u32)) "-" else std.fmt.bufPrint(&cells[1], "{d} ms ago", .{contact}) catch "?";
        const lag_s = if (match == std.math.maxInt(u64)) "-" else std.fmt.bufPrint(&cells[2], "{d}", .{commit -| match}) catch "?";
        const id_s = std.fmt.bufPrint(&cells[3], "{d}", .{id}) catch "?";
        if (json_output) {
            if (i > 0) ctx.print(",", .{});
            ctx.print("{{\"id\":{d},\"role\":\"{s}\",\"address\":\"{s}\"", .{ id, if (m[4] == 0) "voter" else role, address });
            if (contact != std.math.maxInt(u32)) ctx.print(",\"last_contact_ms\":{d}", .{contact});
            if (match != std.math.maxInt(u64)) ctx.print(",\"lag\":{d}", .{commit -| match});
            ctx.print("}}", .{});
        } else {
            table.addRow(&.{ id_s, role, address, contact_s, lag_s }) catch return error.OutOfMemory;
        }
    }
    const removed_at = 14 + @as(usize, count) * 23;
    if (json_output) {
        ctx.print("],\"removed\":[", .{});
        for (0..removed_count) |i| {
            const rm = data[removed_at + i * 12 ..][0..12];
            if (i > 0) ctx.print(",", .{});
            ctx.print("{{\"id\":{d},\"removed_at_ms\":{d}}}", .{ std.mem.readInt(u32, rm[0..4], .little), std.mem.readInt(u64, rm[4..12], .little) });
        }
        ctx.print("]}}\n", .{});
        return;
    }
    for (0..removed_count) |i| {
        const rm = data[removed_at + i * 12 ..][0..12];
        const id_s = std.fmt.bufPrint(&cells[3], "{d}", .{std.mem.readInt(u32, rm[0..4], .little)}) catch "?";
        table.addRow(&.{ id_s, "removed", "-", "-", "-" }) catch return error.OutOfMemory;
    }
    ctx.print("\n", .{});
    table.print(ctx);
    if (leader_id == 0) ctx.print("\nNo leader known; contact and lag are the leader's to report.\n", .{});
    ctx.print("\n", .{});
}

fn runPromote(ctx: *commander.Context) commander.Error!void {
    if (ctx.args.len == 0) {
        return outcome.usage(ctx, "give the id of a replica to promote: flo cluster promote <id>...", .{});
    }
    // One change at a time: each waits until the one before has committed.
    for (ctx.args) |arg| {
        const id = std.fmt.parseInt(u32, arg, 10) catch return outcome.usage(ctx, "{s} is not a node id", .{arg});
        var value: [4]u8 = undefined;
        std.mem.writeInt(u32, &value, id, .little);
        try sendChange(ctx, .cluster_promote, &value);
        ctx.print("node {d} is a voter\n", .{id});
    }
}

fn runRemove(ctx: *commander.Context) commander.Error!void {
    if (ctx.args.len != 1) {
        return outcome.usage(ctx, "give one node id: flo cluster remove <id> [--yes]", .{});
    }
    const id = std.fmt.parseInt(u32, ctx.args[0], 10) catch return outcome.usage(ctx, "{s} is not a node id", .{ctx.args[0]});
    var value: [5]u8 = undefined;
    std.mem.writeInt(u32, value[0..4], id, .little);
    value[4] = @intFromBool(ctx.getBool("yes"));
    try sendChange(ctx, .cluster_remove, &value);
    ctx.print("node {d} removed; it cannot rejoin under that id\n", .{id});
}

/// Send a membership change; it is answered once it commits.
fn sendChange(ctx: *commander.Context, op: proto.OpCode, value: []const u8) commander.Error!void {
    var response = try ask(ctx, op, value);
    response.deinit();
}

// ==================== Testing ====================

test "create cluster command" {
    const allocator = std.testing.allocator;

    const cmd = try createClusterCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("cluster", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 2);
}
