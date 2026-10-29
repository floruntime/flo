//! Cluster management commands for Flo CLI
//!
//! Usage:
//!   flo cluster status [--endpoint <host:port>]     Show cluster health
//!   flo cluster members [--endpoint <host:port>]    List cluster members
//!   flo cluster promote <id>...                     Make caught-up replicas voters
//!   flo cluster remove <id> [--yes]                 Remove a member
//!   flo cluster repair skip --index I [--yes]       Skip the entry the group halted at

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const proto = @import("../../protocol/proto.zig");
const output = @import("../output.zig");
const cli_config = @import("../config.zig");
const outcome = @import("../outcome.zig");
const client_mod = @import("../client/mod.zig");
const wire = @import("../../protocol/cluster_wire.zig");
const server_cmd = @import("server.zig");
const stdx = @import("stdx");
const EntryType = @import("../../storage/ual/entry.zig").EntryType;

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
        .subcommand(
            commander.newBuilder(allocator)
                .name("repair")
                .about("Repair a halted cluster")
                .subcommand(
                commander.newBuilder(allocator)
                    .name("skip")
                    .about("Skip the entry the cluster halted at")
                    .longAbout(
                        \\When every node fails the same write the same way, the cluster
                        \\halts at it and takes no more writes. This skips that write: it
                        \\never runs, on this build or a later one, and the cluster goes on.
                        \\
                        \\Run it on any member: it goes to the node's admin socket, then to the
                        \\leader. The leader skips only the entry it is halted at itself, and
                        \\only once a majority of voters say they are halted there too. Without
                        \\--yes it shows the entry and who is halted at it, and changes nothing.
                    )
                    .uintFlag("index", 0, 0, "The entry the cluster halted at (flo cluster members shows it)")
                    .stringFlag("reason", 0, "", "Why, recorded with the repair")
                    .boolFlag("yes", 0, "Skip it")
                    .stringFlag("config", 'c', "", "The node's config, naming its data directory or [admin] socket")
                    .stringFlag("data-dir", 'd', "", "The node's data directory (overrides the config)")
                    .examples(&.{
                        "flo cluster repair skip --index 18234 --data-dir /var/lib/flo",
                        "flo cluster repair skip --index 18234 --data-dir /var/lib/flo --reason \"malformed invoke\" --yes",
                    })
                    .action(wrapHandler(runRepairSkip)),
            ),
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

    const status = wire.Status.decode(response.data) catch return outcome.malformed(ctx, "cluster status");
    const node_id = status.node_id;
    const leader_id = status.leader_id;
    const term = status.term;
    const state = status.state;
    const member_count = status.member_count;
    const vouched_index = status.vouched_index;
    const strangers_while_quiet = status.strangers_while_quiet;

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

    if (json_output) {
        ctx.print("{{\"node_id\":{f},\"address\":{f},\"leader_id\":", .{ output.jsonString(node_name), output.jsonString(endpoint) });
        // No leader is known while electing or joining.
        if (leader_id == 0) ctx.print("null", .{}) else ctx.print("{f}", .{output.jsonString(leader_name)});
        ctx.print(",\"term\":{d},\"role\":{f},\"members\":{d},\"vouched_index\":{d},\"strangers_while_quiet\":{}}}\n", .{ term, output.jsonString(role_str), member_count, vouched_index, strangers_while_quiet });
    } else {
        ctx.print("\nCluster Status\n", .{});
        ctx.print("──────────────\n", .{});
        ctx.print("Node ID:    {s}\n", .{node_name});
        ctx.print("Address:    {s}\n", .{endpoint});
        ctx.print("Role:       {s}\n", .{role_str});
        ctx.print("Leader:     {s}\n", .{leader_shown});
        ctx.print("Term:       {d}\n", .{term});
        ctx.print("Members:    {d}\n", .{member_count});
        if (vouched_index != 0) ctx.print("\nThis node missed a membership change and is catching up: another member vouched for the config\ncommitted at index {d}. Until its own log reaches it, it also takes appends and vote requests from that config's members.\n", .{vouched_index});
        if (strangers_while_quiet) ctx.print("\nThis node is dropping frames from a node no config of its names, and has heard no leader:\nit may be stuck on an old membership config. Check that a member its last config names is reachable.\n", .{});
        if (state == 5 or state == 6) ctx.print("\nThis node has no log or no hard state of its own (new, or lost). It votes only once a leader has caught\nit up and the members have confirmed the term. If a majority lost their data, see flo server inspect / force-members.\n", .{});
        if (state == 4) ctx.print("\nThis node's data diverged from the group and it takes no writes.\nStop it, move its data directory aside, and start it again with --join <a live member>.\nIt rejoins with no log and votes only once it has caught up.\n", .{});
        ctx.print("\n", .{});
    }
}

fn runMembers(ctx: *commander.Context) commander.Error!void {
    const json_output = output.getFormat(ctx) == .json;

    var response = try ask(ctx, .cluster_members, "");
    defer response.deinit();
    const list = wire.Members.decode(response.data) catch return outcome.malformed(ctx, "member list");

    var table = output.Table.init(ctx.allocator);
    defer table.deinit();
    if (!json_output) {
        for ([_][]const u8{ "ID", "ROLE", "ADDRESS", "LAST CONTACT", "LAG", "HALTED AT" }) |col| table.addColumn(col, .left) catch return error.OutOfMemory;
    } else {
        ctx.print("{{\"leader\":{d},\"members\":[", .{list.leader});
    }
    var cells: [6][48]u8 = undefined;
    var unknown_halts = false;
    for (0..list.count()) |i| {
        const m = list.member(i) catch unreachable; // checked by decode
        const role: []const u8 = switch (m.role) {
            0 => if (m.id == list.leader) "voter (leader)" else "voter",
            1 => "replica",
            2 => "joining",
            else => "unknown",
        };
        const address = if (m.port != 0) std.fmt.bufPrint(&cells[0], "{d}.{d}.{d}.{d}:{d}", .{ m.ip4[0], m.ip4[1], m.ip4[2], m.ip4[3], m.port }) catch "?" else "-";
        const contact_s = if (m.contact_ago_ms == wire.Member.UNKNOWN_CONTACT) "-" else std.fmt.bufPrint(&cells[1], "{d} ms ago", .{m.contact_ago_ms}) catch "?";
        const lag_s = if (m.match == wire.Member.UNKNOWN_MATCH) "-" else std.fmt.bufPrint(&cells[2], "{d}", .{list.commit -| m.match}) catch "?";
        const id_s = std.fmt.bufPrint(&cells[3], "{d}", .{m.id}) catch "?";
        const halt_s = switch (m.halt) {
            .none => "-",
            .unknown => "?",
            .at => |h| std.fmt.bufPrint(&cells[4], "{d} (term {d})", .{ h.index, h.term }) catch "?",
        };
        unknown_halts = unknown_halts or m.halt == .unknown;
        if (json_output) {
            if (i > 0) ctx.print(",", .{});
            ctx.print("{{\"id\":{d},\"role\":{f},\"address\":{f}", .{ m.id, output.jsonString(if (m.role == 0) "voter" else role), output.jsonString(address) });
            if (m.contact_ago_ms != wire.Member.UNKNOWN_CONTACT) ctx.print(",\"last_contact_ms\":{d}", .{m.contact_ago_ms});
            if (m.match != wire.Member.UNKNOWN_MATCH) ctx.print(",\"lag\":{d}", .{list.commit -| m.match});
            switch (m.halt) {
                .none => ctx.print(",\"halt\":null", .{}),
                .unknown => ctx.print(",\"halt\":\"unknown\"", .{}),
                .at => |h| ctx.print(",\"halt\":{{\"index\":{d},\"term\":{d},\"crc\":{d}}}", .{ h.index, h.term, h.crc }),
            }
            ctx.print("}}", .{});
        } else {
            table.addRow(&.{ id_s, role, address, contact_s, lag_s, halt_s }) catch return error.OutOfMemory;
        }
    }
    if (json_output) {
        ctx.print("],\"removed\":[", .{});
        for (0..list.removedCount()) |i| {
            const rm = list.removed(i);
            if (i > 0) ctx.print(",", .{});
            ctx.print("{{\"id\":{d},\"removed_at_ms\":{d}}}", .{ rm.id, rm.when_ms });
        }
        ctx.print("]}}\n", .{});
        return;
    }
    for (0..list.removedCount()) |i| {
        const id_s = std.fmt.bufPrint(&cells[3], "{d}", .{list.removed(i).id}) catch "?";
        table.addRow(&.{ id_s, "removed", "-", "-", "-", "-" }) catch return error.OutOfMemory;
    }
    ctx.print("\n", .{});
    table.print(ctx);
    if (list.leader == 0) {
        ctx.print("\nNo leader known; contact, lag and the others' halts are the leader's to report.\n", .{});
    } else if (unknown_halts) {
        ctx.print("\n?: only the leader hears the members' halts; ask it, or a member it hasn't heard from lately.\n", .{});
    }
    ctx.print("\n", .{});
}

fn runPromote(ctx: *commander.Context) commander.Error!void {
    if (ctx.args.len == 0) {
        return outcome.usage(ctx, "give the id of a replica to promote: flo cluster promote <id>...", .{});
    }
    // One change at a time: each waits until the one before has committed.
    for (ctx.args) |arg| {
        const id = std.fmt.parseInt(u32, arg, 10) catch return outcome.usage(ctx, "{s} is not a node id", .{arg});
        var value: [wire.Promote.SIZE]u8 = undefined;
        try sendChange(ctx, .cluster_promote, (wire.Promote{ .id = id }).encode(&value));
        ctx.print("node {d} is a voter\n", .{id});
    }
}

fn runRemove(ctx: *commander.Context) commander.Error!void {
    if (ctx.args.len != 1) {
        return outcome.usage(ctx, "give one node id: flo cluster remove <id> [--yes]", .{});
    }
    const id = std.fmt.parseInt(u32, ctx.args[0], 10) catch return outcome.usage(ctx, "{s} is not a node id", .{ctx.args[0]});
    var value: [wire.Remove.SIZE]u8 = undefined;
    try sendChange(ctx, .cluster_remove, (wire.Remove{ .id = id, .yes = ctx.getBool("yes") }).encode(&value));
    ctx.print("node {d} removed; it cannot rejoin under that id\n", .{id});
}

/// Send a membership change; it is answered once it commits.
fn sendChange(ctx: *commander.Context, op: proto.OpCode, value: []const u8) commander.Error!void {
    var response = try ask(ctx, op, value);
    response.deinit();
}

fn runRepairSkip(ctx: *commander.Context) commander.Error!void {
    const index = ctx.getUint("index") orelse 0;
    if (index == 0) return outcome.usage(ctx, "--index is required: the entry the cluster halted at (flo cluster members shows it, as HALTED AT)", .{});
    const reason = ctx.getString("reason") orelse "";
    if (reason.len > wire.RepairSkip.MAX_REASON) return outcome.usage(ctx, "--reason is {d} bytes; at most {d}", .{ reason.len, wire.RepairSkip.MAX_REASON });
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try server_cmd.adminSocketPath(ctx, &path_buf);
    if (path.len > stdx.net.UNIX_PATH_MAX) {
        return outcome.localRefusal(ctx, "the admin socket {s} is {d} bytes, past the {d} a Unix socket path may hold, so the node started without one. Set [admin] socket to a shorter path in its config and restart it", .{ path, path.len, stdx.net.UNIX_PATH_MAX });
    }

    // The dry run first, so what is skipped is always shown.
    var dry = try askAdmin(ctx, path, .{ .index = index, .yes = false, .reason = reason });
    defer dry.deinit();
    var ids: [32]u32 = undefined;
    const would = wire.RepairSkipAnswer.decode(dry.data, &ids) catch return outcome.malformed(ctx, "repair answer");
    const type_name = std.enums.tagName(EntryType, @fromBackingInt(would.entry_type)) orelse "unknown";
    ctx.print("\nentry {d}: {s}, term {d}, {d} bytes\n", .{ would.entry.index, type_name, would.entry.term, would.entry_size });
    ctx.print("halted there: node(s) {any}, of {d} voters\n", .{ would.halted, would.voters });
    if (!ctx.getBool("yes")) {
        ctx.print("Would append a repair that skips it: the write never runs, on this build or a later one. Run again with --yes to do it.\n", .{});
        return;
    }
    var done = try askAdmin(ctx, path, .{ .index = index, .yes = true, .reason = reason });
    defer done.deinit();
    const skipped = wire.RepairSkipAnswer.decode(done.data, &ids) catch return outcome.malformed(ctx, "repair answer");
    if (!skipped.skipped) return outcome.malformed(ctx, "repair answer");
    ctx.print("Entry {d} is skipped (repair at index {d}). Each node passes over it as the repair reaches it.\n", .{ index, skipped.skip_index });
}

/// Send a repair skip to the node's admin socket at `path`; its ok answer,
/// which the caller frees.
fn askAdmin(ctx: *commander.Context, path: []const u8, body: wire.RepairSkip) commander.Error!client_mod.Response {
    var client = client_mod.Client.init(ctx.allocator, path);
    defer client.deinit();
    client.connectUnix(path) catch |err| return switch (err) {
        error.ConnectionRefused => outcome.localRefusal(ctx, "no server listens on the admin socket {s}: is a node running on this data directory? One that started without its socket logged why", .{path}),
        error.PermissionDenied => outcome.localRefusal(ctx, "permission denied on the admin socket {s}: run as the user the server runs as", .{path}),
        // Another user's socket could answer as the server would.
        error.NotOurs => outcome.localRefusal(ctx, "{s} isn't a socket you own, so it isn't your server's admin socket: run as the user the server runs as, or check [admin] socket", .{path}),
        else => outcome.localRefusal(ctx, "cannot connect to the admin socket {s}: {s}", .{ path, @errorName(err) }),
    };
    var buf: [wire.RepairSkip.MAX_SIZE]u8 = undefined;
    const value = body.encode(&buf) catch unreachable; // the reason's length is checked
    var response = client.sendRequest(.cluster_repair_skip, "", "", value) catch |err| return outcome.requestFailed(ctx, err);
    outcome.check(ctx, response) catch |err| {
        response.deinit();
        return err;
    };
    return response;
}

// ==================== Testing ====================

test "create cluster command" {
    const allocator = std.testing.allocator;

    const cmd = try createClusterCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("cluster", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 2);
}
