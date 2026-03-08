//! Queue commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo queue enqueue <queue> <payload> [--priority <0-255>] [--delay <ms>]
//!   flo queue dequeue <queue> [--count <n>] [--timeout <ms>]
//!   flo queue watch <queue>              - Continuously watch for messages
//!   flo queue peek <queue> [--count <n>]
//!   flo queue ack <queue> <seq>...
//!   flo queue nack <queue> <seq>... [--dlq]

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const output = @import("../output.zig");
const wire = @import("../../util/wire.zig");
const WireReader = wire.WireReader;
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

/// Create the queue command tree
pub fn createQueueCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("queue")
        .about("Queue operations")
        .group("Data Commands")
        .longAbout(
            \\Interact with Flo's message queue system.
            \\
            \\Provides commands for enqueueing, dequeueing, and managing
            \\messages with support for priority, delays, and acknowledgements.
        )
        // Persistent flags - inherited by all subcommands
        .persistentFlag("namespace", .{ .short = 'n', .value = .{ .string = "default" }, .desc = "Namespace to use" })
        .persistentFlag("endpoint", .{ .short = 'e', .value = .{ .string = "" }, .desc = "Server endpoint (host:port)" })
        .subcommand(
            commander.newBuilder(allocator)
                .name("enqueue")
                .about("Add a message to a queue")
                .aliases(&.{"push"})
                .examples(&.{
                    "flo queue enqueue myqueue 'Hello, World!'",
                    "flo queue enqueue tasks '{\"id\":1}' --priority 10",
                    "flo queue enqueue jobs payload --delay 5000",
                })
                .arg("queue", "Queue name")
                .arg("payload", "Message payload")
                .uintFlag("priority", 'p', 0, "Priority (0-255, higher=more urgent)")
                .uintFlag("delay", 'd', 0, "Delay before visible (ms)")
                .action(wrapHandler(runEnqueue)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("dequeue")
                .about("Get messages from a queue")
                .aliases(&.{"pop"})
                .examples(&.{
                    "flo queue dequeue myqueue",
                    "flo queue dequeue myqueue --count 10",
                    "flo queue dequeue myqueue --timeout 5000",
                })
                .arg("queue", "Queue name")
                .uintFlag("count", 'c', 1, "Number of messages to dequeue")
                .uintFlag("timeout", 't', 30000, "Visibility timeout (ms)")
                .uintFlag("block", 'b', 0, "Block timeout (ms, 0=no block)")
                .action(wrapHandler(runDequeue)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("watch")
                .about("Continuously watch a queue")
                .arg("queue", "Queue name")
                .action(wrapHandler(runWatch)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("peek")
                .about("View messages without removing")
                .arg("queue", "Queue name")
                .uintFlag("count", 'c', 1, "Number of messages to peek")
                .action(wrapHandler(runPeek)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("touch")
                .about("Extend message visibility timeout")
                .arg("queue", "Queue name")
                .arg("seq", "Sequence number(s)")
                .uintFlag("extend", 'e', 0, "New visibility timeout in milliseconds (default: reset to original timeout)")
                .action(wrapHandler(runTouch)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("ack")
                .about("Acknowledge message processing")
                .aliases(&.{"complete"})
                .arg("queue", "Queue name")
                .arg("seq", "Sequence number(s)")
                .action(wrapHandler(runAck)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("nack")
                .about("Negative-acknowledge (requeue or DLQ)")
                .aliases(&.{"fail"})
                .arg("queue", "Queue name")
                .arg("seq", "Sequence number(s)")
                .boolFlag("dlq", 0, "Send to dead-letter queue")
                .action(wrapHandler(runNack)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("dlq")
                .about("Dead-letter queue operations")
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("list")
                        .about("List DLQ messages")
                        .arg("queue", "Queue name")
                        .uintFlag("limit", 'l', 100, "Maximum messages")
                        .action(wrapHandler(runDlqList)),
                )
                .subcommand(
                commander.newBuilder(allocator)
                    .name("requeue")
                    .about("Requeue DLQ messages")
                    .arg("queue", "Queue name")
                    .arg("seq", "Sequence number(s)")
                    .action(wrapHandler(runDlqRequeue)),
            ),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List all queues")
                .aliases(&.{"ls"})
                .uintFlag("limit", 'l', 100, "Maximum queues to list")
                .boolFlag("json", 'j', "Output in JSON format")
                .action(wrapHandler(runList)),
        )
        .build();
}



fn runEnqueue(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const payload = ctx.getPositional("payload").?; // validated by commander

    const priority_val = ctx.getUint("priority") orelse 0;
    const priority: u8 = if (priority_val > 255) 255 else @intCast(priority_val);
    const delay = ctx.getUint("delay");
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // enqueue(client, namespace, queue, payload, priority, delay_ms, dedup_key)
    var result = client_mod.queue.enqueue(&client, namespace, queue, payload, priority, if (delay) |d| @as(u64, d) else null, null) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    // Response data may contain seq number
    if (result.asString()) |data| {
        if (data.len > 0) {
            ctx.print("Enqueued: {s}\n", .{data});
            return;
        }
    }
    ctx.print("OK\n", .{});
}

fn runDequeue(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander

    const count = ctx.getUint("count") orelse 1;
    const timeout = ctx.getUint("timeout") orelse 30000;
    const block = ctx.getChangedUint("block");
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    // dequeue(client, namespace, queue, count, timeout_ms, block_ms)
    var result = client_mod.queue.dequeue(&client, namespace, queue, @intCast(count), @intCast(timeout), if (block) |b| @as(u32, @intCast(b)) else null) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    // Parse wire format: [count:u32] ([seq:u64][payload_len:u32][payload][enqueued_at:i64][delivery_count:u32][priority:u8])*
    if (result.data.len == 0) {
        ctx.print("(no messages)\n", .{});
        return;
    }

    var reader = WireReader.init(result.data);
    const msg_count = reader.readU32() orelse {
        ctx.print("(no messages)\n", .{});
        return;
    };

    if (msg_count == 0) {
        ctx.print("(no messages)\n", .{});
        return;
    }

    var i: u32 = 0;
    while (i < msg_count) : (i += 1) {
        _ = reader.readU64() orelse break; // seq
        const payload = reader.readLengthPrefixed(u32) orelse break;
        _ = reader.readI64() orelse break; // enqueued_at
        _ = reader.readU32() orelse break; // delivery_count
        _ = reader.readU8() orelse break; // priority

        // Output just the payload
        ctx.print("{s}\n", .{payload});
    }
}

fn runWatch(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander

    const namespace = cli_config.getNamespace(ctx);
    ctx.print("Watching queue: {s}\n", .{queue});
    ctx.print("Press Ctrl+C to stop.\n\n", .{});

    // Continuous polling loop
    const endpoint = cli_config.getEndpoint(ctx);
    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    while (true) {
        // Block for messages with 1 second timeout
        var result = client_mod.queue.dequeue(&client, namespace, queue, 1, 30000, 1000) catch |err| {
            ctx.printErr("Request failed: {}\n", .{err});
            std.Thread.sleep(1 * std.time.ns_per_s);
            continue;
        };
        defer result.deinit();

        if (result.data.len == 0) continue;

        // Parse wire format
        var reader = WireReader.init(result.data);
        const msg_count = reader.readU32() orelse continue;
        if (msg_count == 0) continue;

        var i: u32 = 0;
        while (i < msg_count) : (i += 1) {
            _ = reader.readU64() orelse break; // seq
            const payload = reader.readLengthPrefixed(u32) orelse break;
            _ = reader.readI64() orelse break; // enqueued_at
            _ = reader.readU32() orelse break; // delivery_count
            _ = reader.readU8() orelse break; // priority

            ctx.print("{s}\n", .{payload});
        }
    }
}

fn runPeek(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander

    const count = ctx.getUint("count") orelse 1;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    var result = client_mod.queue.peek(&client, namespace, queue, @intCast(count)) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    if (result.asString()) |data| {
        if (data.len > 0) {
            ctx.print("{s}\n", .{data});
        } else {
            ctx.print("(empty)\n", .{});
        }
    } else {
        ctx.print("(empty)\n", .{});
    }
}

fn runTouch(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const seq_str = ctx.getPositional("seq").?; // validated by commander

    const seq = std.fmt.parseInt(u64, seq_str, 10) catch {
        ctx.printErr("Error: Invalid sequence number\n", .{});
        return error.CommandFailed;
    };

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const extend_ms: u32 = @intCast(ctx.getUint("extend").?);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // touch(client, namespace, queue, seqs, extend_ms)
    var result = client_mod.queue.touch(&client, namespace, queue, &[_]u64{seq}, extend_ms) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    ctx.print("OK\n", .{});
}

fn runAck(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const seq_str = ctx.getPositional("seq").?; // validated by commander

    const seq = std.fmt.parseInt(u64, seq_str, 10) catch {
        ctx.printErr("Error: Invalid sequence number\n", .{});
        return;
    };

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.queue.ack(&client, namespace, queue, &[_]u64{seq}) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("OK\n", .{});
}

fn runNack(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const seq_str = ctx.getPositional("seq").?; // validated by commander

    const seq = std.fmt.parseInt(u64, seq_str, 10) catch {
        ctx.printErr("Error: Invalid sequence number\n", .{});
        return error.CommandFailed;
    };

    const to_dlq = ctx.getBool("dlq");
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.queue.nack(&client, namespace, queue, &[_]u64{seq}, to_dlq) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    ctx.print("OK\n", .{});
}

fn runDlqList(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander

    const limit = ctx.getUint("limit") orelse 100;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    var result = client_mod.queue.dlqList(&client, namespace, queue, @intCast(limit)) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    ctx.print("Dead-letter queue for: {s}\n", .{queue});
    if (result.asString()) |data| {
        if (data.len > 0) {
            ctx.print("{s}\n", .{data});
        } else {
            ctx.print("(empty)\n", .{});
        }
    } else {
        ctx.print("(empty)\n", .{});
    }
}

fn runDlqRequeue(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const seq_str = ctx.getPositional("seq").?; // validated by commander

    const seq = std.fmt.parseInt(u64, seq_str, 10) catch {
        ctx.printErr("Error: Invalid sequence number\n", .{});
        return;
    };

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.queue.dlqRequeue(&client, namespace, queue, &[_]u64{seq}) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return;
    }

    ctx.print("Requeued: seq={d}\n", .{seq});
}

// ==================== Testing ====================

test "create queue command" {
    const allocator = std.testing.allocator;

    const cmd = try createQueueCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("queue", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 6);
}

// ==================== Queue List ====================

const QueueListEntry = struct {
    name: []const u8,
    namespace: []const u8,
    pending: u64,
    available: u64,
    enqueued: u64,
    dequeued: u64,
    dlq: u64,
};

fn runList(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const limit = ctx.getUint("limit") orelse 100;
    const json_output = ctx.getBool("json");

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // Collect all queues from all shards via cursor-based shard walking
    var all_queues: std.ArrayList(QueueListEntry) = .empty;
    defer {
        for (all_queues.items) |entry| {
            ctx.allocator.free(entry.name);
            ctx.allocator.free(entry.namespace);
        }
        all_queues.deinit(ctx.allocator);
    }

    var cursor: ?[]const u8 = null;
    var cursor_owned: ?[]u8 = null;
    defer if (cursor_owned) |c| ctx.allocator.free(c);

    // Walk all shards until no more data
    while (all_queues.items.len < limit) {
        var response = client_mod.queue.list(&client, namespace, @intCast(limit), cursor) catch |err| {
            ctx.printErr("Request failed: {}\n", .{err});
            return error.CommandFailed;
        };
        defer response.deinit();

        if (response.isError()) {
            ctx.printErr("Error: {s}\n", .{response.errorMessage()});
            return error.CommandFailed;
        }

        // Parse response data - wire format:
        // [count:u32] ([name_len:u32][name][ns_len:u32][ns][pending:u64][available:u64][enqueued:u64][dequeued:u64][dlq:u64])* [has_more:u8] [cursor_len:u16][cursor]?
        if (response.data.len < 4) break;

        var reader = WireReader.init(response.data);
        const count = reader.readU32() orelse break;

        // Read queues
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            const name_len = reader.readU32() orelse break;
            const name = reader.readSlice(name_len) orelse break;
            const ns_len = reader.readU32() orelse break;
            const ns = reader.readSlice(ns_len) orelse break;
            const pending = reader.readU64() orelse break;
            const available = reader.readU64() orelse break;
            const enqueued = reader.readU64() orelse break;
            const dequeued = reader.readU64() orelse break;
            const dlq_count = reader.readU64() orelse break;

            // Dedup across shards (same queue name shouldn't appear on multiple shards,
            // but guard against it)
            var found = false;
            for (all_queues.items) |entry| {
                if (std.mem.eql(u8, entry.name, name) and std.mem.eql(u8, entry.namespace, ns)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                const name_copy = ctx.allocator.dupe(u8, name) catch break;
                const ns_copy = ctx.allocator.dupe(u8, ns) catch {
                    ctx.allocator.free(name_copy);
                    break;
                };
                all_queues.append(ctx.allocator, .{
                    .name = name_copy,
                    .namespace = ns_copy,
                    .pending = pending,
                    .available = available,
                    .enqueued = enqueued,
                    .dequeued = dequeued,
                    .dlq = dlq_count,
                }) catch {
                    ctx.allocator.free(name_copy);
                    ctx.allocator.free(ns_copy);
                    break;
                };
            }

            if (all_queues.items.len >= limit) break;
        }

        // Read has_more flag
        const has_more = (reader.readU8() orelse 0) != 0;

        // Read next cursor
        const cursor_len = reader.readU16() orelse 0;
        const next_cursor = if (cursor_len > 0) reader.readSlice(cursor_len) else null;

        // Free previous cursor and copy new one
        if (cursor_owned) |c| ctx.allocator.free(c);
        cursor_owned = null;

        if (!has_more or next_cursor == null) {
            break; // Done walking shards
        }

        // Copy cursor for next iteration
        cursor_owned = ctx.allocator.dupe(u8, next_cursor.?) catch break;
        cursor = cursor_owned;
    }

    // Output results
    if (all_queues.items.len == 0) {
        if (json_output) {
            ctx.print("[]\n", .{});
        } else {
            ctx.print("No queues found in namespace '{s}'\n", .{namespace});
        }
        return;
    }

    if (json_output) {
        ctx.print("[", .{});
        for (all_queues.items, 0..) |entry, idx| {
            if (idx > 0) ctx.print(",", .{});
            ctx.print("{{\"name\":\"{s}\",\"namespace\":\"{s}\",\"pending\":{d},\"available\":{d},\"enqueued\":{d},\"dequeued\":{d},\"dlq\":{d}}}", .{ entry.name, entry.namespace, entry.pending, entry.available, entry.enqueued, entry.dequeued, entry.dlq });
        }
        ctx.print("]\n", .{});
    } else {
        ctx.print("Queues in namespace '{s}':\n", .{namespace});
        ctx.print("{s:<20} {s:<12} {s:<10} {s:<10} {s:<10} {s:<10} {s:<6}\n", .{ "NAME", "NAMESPACE", "PENDING", "AVAILABLE", "ENQUEUED", "DEQUEUED", "DLQ" });
        ctx.print("{s}\n", .{"-" ** 78});
        for (all_queues.items) |entry| {
            ctx.print("{s:<20} {s:<12} {d:<10} {d:<10} {d:<10} {d:<10} {d:<6}\n", .{ entry.name, entry.namespace, entry.pending, entry.available, entry.enqueued, entry.dequeued, entry.dlq });
        }
    }
}
