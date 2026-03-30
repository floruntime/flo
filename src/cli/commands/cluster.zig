//! Cluster management commands for Flo CLI
//!
//! Usage:
//!   flo cluster status [--endpoint <host:port>]     Show cluster health
//!   flo cluster members [--endpoint <host:port>]    List cluster members
//!   flo cluster transfer-leader <node-id>           Transfer leadership

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const proto = @import("../../protocol/proto.zig");
const output = @import("../output.zig");
const cli_config = @import("../config.zig");
const net = std.net;

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
                    "flo cluster status --json",
                })
                .boolFlag("json", 'j', "Output in JSON format")
                .action(wrapHandler(runStatus)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("members")
                .about("List all cluster members")
                .examples(&.{
                    "flo cluster members",
                    "flo cluster members --endpoint localhost:9000",
                    "flo cluster members --json",
                })
                .boolFlag("json", 'j', "Output in JSON format")
                .action(wrapHandler(runMembers)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("transfer-leader")
                .about("Transfer leadership to another node")
                .examples(&.{
                    "flo cluster transfer-leader 2",
                    "flo cluster transfer-leader 2 --endpoint localhost:9000",
                })
                .action(wrapHandler(runTransferLeader)),
        )
        .build();
}

/// Parse endpoint string into host and port
fn parseEndpoint(endpoint: []const u8) !struct { host: []const u8, port: u16 } {
    const colon_pos = std.mem.lastIndexOfScalar(u8, endpoint, ':') orelse {
        return .{ .host = endpoint, .port = 9000 };
    };
    const port = std.fmt.parseInt(u16, endpoint[colon_pos + 1 ..], 10) catch 9000;
    return .{ .host = endpoint[0..colon_pos], .port = port };
}

/// Format a node ID as a human-readable name.
/// Generates "flo-XXXXXX" format using lower 24 bits of node_id.
fn formatNodeId(buf: *[11]u8, node_id: u32) []const u8 {
    // Use lower 24 bits for 6 hex chars (16.7M combinations)
    const short_hash: u24 = @truncate(node_id);
    _ = std.fmt.bufPrint(buf, "flo-{x:0>6}", .{short_hash}) catch "flo-000000";
    return buf[0..10];
}

/// Response with owned buffer
const OwnedResponse = struct {
    response: proto.Response,
    buffer: []u8,
    allocator: Allocator,

    pub fn deinit(self: *OwnedResponse) void {
        self.allocator.free(self.buffer);
    }
};

/// Send a Flo protocol request and receive response
fn sendRequest(
    allocator: Allocator,
    host: []const u8,
    port: u16,
    op_code: proto.OpCode,
    value: []const u8,
) !OwnedResponse {
    // Connect to server
    const stream = try net.tcpConnectToHost(allocator, host, port);
    defer stream.close();

    // Build request
    const request = proto.Request{
        .header = .{
            .magic = proto.MAGIC,
            .version = proto.VERSION,
            .op_code = @intFromEnum(op_code),
            .flags = 0,
            .reserved = .{0} ** 8,
            .payload_length = 0, // Will be set during serialization
            .request_id = 1, // Simple request ID for CLI
            .crc32 = 0, // Will be computed during serialization
        },
        .namespace = "", // Cluster ops don't need namespace
        .key = "",
        .value = value,
        .options = "",
    };

    // Serialize and send
    var send_buf: [1024]u8 = undefined;
    const serialized = try request.serialize(&send_buf);
    try stream.writeAll(serialized);

    // Read response header
    var header_buf: [@sizeOf(proto.ResponseHeader)]u8 = undefined;
    const header_bytes = try stream.readAtLeast(&header_buf, @sizeOf(proto.ResponseHeader));
    if (header_bytes < @sizeOf(proto.ResponseHeader)) {
        return error.IncompleteResponse;
    }

    const header = @as(*align(1) const proto.ResponseHeader, @ptrCast(&header_buf)).*;
    try header.validate();

    // Read response body
    if (header.data_len > 64 * 1024) {
        return error.ResponseTooLarge;
    }

    const resp_buf = try allocator.alloc(u8, @sizeOf(proto.ResponseHeader) + header.data_len);
    errdefer allocator.free(resp_buf);

    @memcpy(resp_buf[0..@sizeOf(proto.ResponseHeader)], &header_buf);

    if (header.data_len > 0) {
        const body_bytes = try stream.readAtLeast(resp_buf[@sizeOf(proto.ResponseHeader)..], header.data_len);
        if (body_bytes < header.data_len) {
            return error.IncompleteResponse;
        }
    }

    const response = try proto.Response.parse(resp_buf[0 .. @sizeOf(proto.ResponseHeader) + header.data_len]);

    return OwnedResponse{
        .response = response,
        .buffer = resp_buf,
        .allocator = allocator,
    };
}

fn runStatus(ctx: *commander.Context) commander.Error!void {
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = ctx.getBool("json");

    const ep = parseEndpoint(endpoint) catch {
        ctx.printErr("Invalid endpoint format: {s}\n", .{endpoint});
        return error.CommandFailed;
    };

    // Send cluster_status request
    var owned = sendRequest(
        ctx.allocator,
        ep.host,
        ep.port,
        .cluster_status,
        "", // No payload needed
    ) catch |err| {
        ctx.printErr("Failed to query cluster status: {}\n", .{err});
        return error.CommandFailed;
    };
    defer owned.deinit();

    const response = owned.response;

    if (response.getStatus() != .ok) {
        // Error message is in response.data
        if (response.data.len > 0) {
            ctx.printErr("Error: {s}\n", .{response.data});
        } else {
            ctx.printErr("Server returned error: {}\n", .{response.getStatus()});
        }
        return error.CommandFailed;
    }

    // Parse response data
    // Expected format: [node_id: u32][leader_id: u32][term: u64][state: u8][member_count: u32]
    if (response.data.len < 21) {
        ctx.printErr("Invalid response format\n", .{});
        return error.CommandFailed;
    }

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
        1 => "candidate",
        2 => "leader",
        else => "unknown",
    };

    if (json_output) {
        ctx.print("{{\"node_id\":\"{s}\",\"address\":\"{s}:{d}\",\"leader_id\":\"{s}\",\"term\":{d},\"role\":\"{s}\",\"members\":{d}}}\n", .{
            node_name,
            ep.host,
            ep.port,
            leader_name,
            term,
            role_str,
            member_count,
        });
    } else {
        ctx.print("\nCluster Status\n", .{});
        ctx.print("──────────────\n", .{});
        ctx.print("Node ID:    {s}\n", .{node_name});
        ctx.print("Address:    {s}:{d}\n", .{ ep.host, ep.port });
        ctx.print("Role:       {s}\n", .{role_str});
        ctx.print("Leader:     {s}\n", .{leader_name});
        ctx.print("Term:       {d}\n", .{term});
        ctx.print("Members:    {d}\n\n", .{member_count});
    }
}

fn runMembers(ctx: *commander.Context) commander.Error!void {
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = ctx.getBool("json");

    const ep = parseEndpoint(endpoint) catch {
        ctx.printErr("Invalid endpoint format: {s}\n", .{endpoint});
        return error.CommandFailed;
    };

    // Send cluster_members request
    var owned = sendRequest(
        ctx.allocator,
        ep.host,
        ep.port,
        .cluster_members,
        "", // No payload needed
    ) catch |err| {
        ctx.printErr("Failed to query cluster members: {}\n", .{err});
        return error.CommandFailed;
    };
    defer owned.deinit();

    const response = owned.response;

    if (response.getStatus() != .ok) {
        // Error message is in response.data
        if (response.data.len > 0) {
            ctx.printErr("Error: {s}\n", .{response.data});
        } else {
            ctx.printErr("Server returned error: {}\n", .{response.getStatus()});
        }
        return error.CommandFailed;
    }

    // Parse response data
    // Format: [count: u32] + [node_id: u32][state: u8][addr_len: u16][addr: bytes]...
    if (response.data.len < 4) {
        ctx.printErr("Invalid response format\n", .{});
        return error.CommandFailed;
    }

    const count = std.mem.readInt(u32, response.data[0..4], .little);

    // Format the endpoint address for local node display
    var endpoint_buf: [64]u8 = undefined;
    const endpoint_addr = std.fmt.bufPrint(&endpoint_buf, "{s}:{d}", .{ ep.host, ep.port }) catch endpoint;

    if (json_output) {
        ctx.print("{{\"members\":[", .{});
    } else {
        // Use Table for clean formatting
        var table = output.Table.init(ctx.allocator);
        defer table.deinit();

        table.addColumn("NODE ID", .left) catch return error.CommandFailed;
        table.addColumn("ADDRESS", .left) catch return error.CommandFailed;
        table.addColumn("ROLE", .left) catch return error.CommandFailed;
        table.addColumn("STATE", .left) catch return error.CommandFailed;

        var offset: usize = 4;
        var i: u32 = 0;
        while (i < count and offset + 7 <= response.data.len) : (i += 1) {
            const member_node_id = std.mem.readInt(u32, response.data[offset..][0..4], .little);
            offset += 4;
            const member_state = response.data[offset];
            offset += 1;
            const addr_len = std.mem.readInt(u16, response.data[offset..][0..2], .little);
            offset += 2;

            if (offset + addr_len > response.data.len) break;
            const addr = response.data[offset..][0..addr_len];
            offset += addr_len;

            // Generate human-readable node name (short display of full hash)
            var member_name_buf: [11]u8 = undefined;
            const member_name = formatNodeId(&member_name_buf, member_node_id);

            // Role from Raft state
            const role_str = switch (member_state) {
                0 => "follower",
                1 => "candidate",
                2 => "leader",
                else => "unknown",
            };

            // Address: use response addr, or endpoint for local node
            const address = if (addr.len > 0) addr else endpoint_addr;

            // For now, state is always "alive" for members we can see
            // TODO: Add actual health status from gossip protocol
            const state_str = "alive";

            table.addRow(&.{ member_name, address, role_str, state_str }) catch return error.CommandFailed;
        }

        ctx.print("\n", .{});
        table.print(ctx);
        ctx.print("\n", .{});
        return;
    }

    // JSON output path
    var offset: usize = 4;
    var i: u32 = 0;
    while (i < count and offset + 7 <= response.data.len) : (i += 1) {
        const member_node_id = std.mem.readInt(u32, response.data[offset..][0..4], .little);
        offset += 4;
        const member_state = response.data[offset];
        offset += 1;
        const addr_len = std.mem.readInt(u16, response.data[offset..][0..2], .little);
        offset += 2;

        if (offset + addr_len > response.data.len) break;
        const addr = response.data[offset..][0..addr_len];
        offset += addr_len;

        // Generate human-readable node name
        var member_name_buf: [11]u8 = undefined;
        const member_name = formatNodeId(&member_name_buf, member_node_id);

        const role_str = switch (member_state) {
            0 => "follower",
            1 => "candidate",
            2 => "leader",
            else => "unknown",
        };

        // Address: use response addr, or endpoint for local node
        const address = if (addr.len > 0) addr else endpoint_addr;
        const state_str = "alive";

        if (i > 0) ctx.print(",", .{});
        ctx.print("{{\"node_id\":\"{s}\",\"address\":\"{s}\",\"role\":\"{s}\",\"state\":\"{s}\"}}", .{
            member_name,
            address,
            role_str,
            state_str,
        });
    }

    ctx.print("]}}\n", .{});
}

fn runTransferLeader(ctx: *commander.Context) commander.Error!void {
    const endpoint = cli_config.getEndpoint(ctx);
    const args = ctx.args;

    if (args.len == 0) {
        ctx.printErr("Error: target node ID required\n", .{});
        ctx.printErr("Usage: flo cluster transfer-leader <node-id>\n", .{});
        return error.CommandFailed;
    }

    const target_node_id = std.fmt.parseInt(u32, args[0], 10) catch {
        ctx.printErr("Invalid node ID: {s}\n", .{args[0]});
        return error.CommandFailed;
    };

    const ep = parseEndpoint(endpoint) catch {
        ctx.printErr("Invalid endpoint format: {s}\n", .{endpoint});
        return error.CommandFailed;
    };

    // Send cluster_transfer_leader request with target node ID
    var value_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &value_buf, target_node_id, .little);

    var owned = sendRequest(
        ctx.allocator,
        ep.host,
        ep.port,
        .cluster_transfer_leader,
        &value_buf,
    ) catch |err| {
        ctx.printErr("Failed to send transfer request: {}\n", .{err});
        return error.CommandFailed;
    };
    defer owned.deinit();

    const response = owned.response;

    if (response.getStatus() == .ok) {
        ctx.print("✓ Leadership transfer to node {d} initiated successfully\n", .{target_node_id});
    } else {
        // Error message is in response.data
        if (response.data.len > 0) {
            ctx.printErr("✗ Leadership transfer failed: {s}\n", .{response.data});
        } else {
            ctx.printErr("✗ Leadership transfer failed: {}\n", .{response.getStatus()});
        }
        return error.CommandFailed;
    }
}

// ==================== Testing ====================

test "create cluster command" {
    const allocator = std.testing.allocator;

    const cmd = try createClusterCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("cluster", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 2);
}

test "parseEndpoint" {
    const ep1 = try parseEndpoint("localhost:9000");
    try std.testing.expectEqualStrings("localhost", ep1.host);
    try std.testing.expectEqual(@as(u16, 9000), ep1.port);

    const ep2 = try parseEndpoint("192.168.1.10:4445");
    try std.testing.expectEqualStrings("192.168.1.10", ep2.host);
    try std.testing.expectEqual(@as(u16, 4445), ep2.port);
}
