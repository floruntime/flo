//! Status command for Flo CLI
//!
//! Usage:
//!   flo status [--endpoint <host:port>]   Check server health and status

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");

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
            \\Makes an HTTP request to the server's health endpoint to verify
            \\the server is running and responding to requests.
            \\
            \\Exit codes:
            \\  0 - Server is healthy
            \\  1 - Server is not responding or unhealthy
        )
        .examples(&.{
            "flo status",
            "flo status --endpoint localhost:9000",
            "flo status -e prod.example.com:9000",
        })
        .stringFlag("endpoint", 'e', "localhost:9000", "Server endpoint (host:port)")
        .boolFlag("json", 'j', "Output in JSON format")
        .action(wrapHandler(runStatus))
        .build();
}

fn runStatus(ctx: *commander.Context) commander.Error!void {
    const endpoint = ctx.getString("endpoint") orelse "localhost:9000";
    const json_output = ctx.getBool("json");

    // Parse host:port
    const colon_pos = std.mem.indexOfScalar(u8, endpoint, ':');
    const host = if (colon_pos) |pos| endpoint[0..pos] else endpoint;
    const port_str = if (colon_pos) |pos| endpoint[pos + 1 ..] else "9000";
    const port = std.fmt.parseInt(u16, port_str, 10) catch 9000;

    const resolved_host = if (std.mem.eql(u8, host, "localhost")) "127.0.0.1" else host;

    if (!json_output) {
        ctx.print("Checking server at {s}:{d}...\n", .{ resolved_host, port });
    }

    // Make HTTP request to /health endpoint
    const address = std.net.Address.parseIp4(resolved_host, port) catch {
        if (json_output) {
            ctx.print("{{\"status\":\"error\",\"message\":\"Invalid address: {s}\"}}\n", .{resolved_host});
        } else {
            ctx.printErr("Error: Invalid address: {s}\n", .{resolved_host});
        }
        return error.CommandFailed;
    };

    const stream = std.net.tcpConnectToAddress(address) catch |err| {
        if (json_output) {
            ctx.print("{{\"status\":\"down\",\"endpoint\":\"{s}:{d}\",\"error\":\"{}\"}}\n", .{ resolved_host, port, err });
        } else {
            ctx.printErr("✗ Server at {s}:{d} is DOWN\n", .{ resolved_host, port });
            ctx.printErr("  Connection failed: {}\n", .{err});
        }
        return error.CommandFailed;
    };
    defer stream.close();

    // Send HTTP GET request to /health
    var request_buf: [256]u8 = undefined;
    const request = std.fmt.bufPrint(&request_buf, "GET /health HTTP/1.1\r\nHost: {s}\r\nConnection: close\r\n\r\n", .{resolved_host}) catch {
        ctx.printErr("Error: Request buffer too small\n", .{});
        return error.CommandFailed;
    };
    _ = stream.write(request) catch |err| {
        if (json_output) {
            ctx.print("{{\"status\":\"error\",\"message\":\"Write failed: {}\"}}\n", .{err});
        } else {
            ctx.printErr("Write failed: {}\n", .{err});
        }
        return error.CommandFailed;
    };

    // Read response
    var buf: [4096]u8 = undefined;
    const n = stream.read(&buf) catch |err| {
        if (json_output) {
            ctx.print("{{\"status\":\"error\",\"message\":\"Read failed: {}\"}}\n", .{err});
        } else {
            ctx.printErr("Read failed: {}\n", .{err});
        }
        return error.CommandFailed;
    };

    // Check for 200 OK
    if (std.mem.startsWith(u8, buf[0..n], "HTTP/1.1 200")) {
        // Extract body (JSON from /health endpoint)
        var body: []const u8 = "";
        if (std.mem.indexOf(u8, buf[0..n], "\r\n\r\n")) |body_start| {
            body = std.mem.trim(u8, buf[body_start + 4 .. n], " \r\n");
        }

        if (json_output) {
            // Pass through the JSON body from /health
            if (body.len > 0) {
                ctx.print("{s}\n", .{body});
            } else {
                ctx.print("{{\"status\":\"healthy\",\"endpoint\":\"{s}:{d}\"}}\n", .{ resolved_host, port });
            }
        } else {
            ctx.print("✓ Server is HEALTHY\n", .{});
            ctx.print("  Endpoint: {s}:{d}\n", .{ resolved_host, port });
            if (body.len > 0) {
                ctx.print("  Response: {s}\n", .{body});
            }
        }
    } else {
        if (json_output) {
            ctx.print("{{\"status\":\"unhealthy\",\"endpoint\":\"{s}:{d}\"}}\n", .{ resolved_host, port });
        } else {
            ctx.print("✗ Server returned: {s}\n", .{buf[0..@min(n, 50)]});
        }
        return error.CommandFailed;
    }
}

// ==================== Testing ====================

test "create status command" {
    const allocator = std.testing.allocator;

    const cmd = try createStatusCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("status", cmd.name);
}
