//! Status command for Flo CLI
//!
//! Sends a binary ping over the wire protocol (port 9000 by default).
//! Works even when the dashboard is disabled.
//!
//! Usage:
//!   flo status [--endpoint <host:port>]   Check server health and status

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const cli_config = @import("../config.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the status command
pub fn createStatusCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("status")
        .about("Check server health and status")
        .group("Server Commands")
        .longAbout(
            \\Check the health and status of a running Flo server.
            \\
            \\Sends a binary ping to the server's wire protocol port to verify
            \\the server is running and responding to requests.
            \\
            \\Exit codes:
            \\  0 - Server is healthy
            \\  1 - Server is not responding or unhealthy
        )
        .examples(&.{
            "flo status",
            "flo status --endpoint 127.0.0.1:9000",
            "flo status -e prod.example.com:9000",
        })
        .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
        .boolFlag("json", 'j', "Output in JSON format")
        .action(wrapHandler(runStatus))
        .build();
}

fn runStatus(ctx: *commander.Context) commander.Error!void {
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = ctx.getBool("json");

    if (!json_output) {
        ctx.print("Checking server at {s}...\n", .{endpoint});
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        if (json_output) {
            ctx.print("{{\"status\":\"down\",\"endpoint\":\"{s}\",\"error\":\"{}\"}}\n", .{ endpoint, err });
        } else {
            ctx.printErr("✗ Server at {s} is DOWN\n", .{endpoint});
            ctx.printErr("  Connection failed: {}\n", .{err});
        }
        return error.CommandFailed;
    };

    var result = client.sendRequest(.ping, "", "", "") catch |err| {
        if (json_output) {
            ctx.print("{{\"status\":\"error\",\"endpoint\":\"{s}\",\"error\":\"{}\"}}\n", .{ endpoint, err });
        } else {
            ctx.printErr("✗ Server at {s} is not responding\n", .{endpoint});
            ctx.printErr("  Request failed: {}\n", .{err});
        }
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        const err_msg = result.errorMessage();
        if (json_output) {
            ctx.print("{{\"status\":\"unhealthy\",\"endpoint\":\"{s}\",\"error\":\"{s}\"}}\n", .{ endpoint, err_msg });
        } else {
            ctx.printErr("✗ Server is UNHEALTHY: {s}\n", .{err_msg});
        }
        return error.CommandFailed;
    }

    if (json_output) {
        ctx.print("{{\"status\":\"healthy\",\"endpoint\":\"{s}\"}}\n", .{endpoint});
    } else {
        ctx.print("✓ Server is HEALTHY\n", .{});
        ctx.print("  Endpoint: {s}\n", .{endpoint});
    }
}

// ==================== Testing ====================

test "create status command" {
    const allocator = std.testing.allocator;

    const cmd = try createStatusCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("status", cmd.name);
}
