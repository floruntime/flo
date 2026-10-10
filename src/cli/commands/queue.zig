//! Queue commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo queue enqueue <queue> <payload> [--priority <0-255>]
//!   flo queue dequeue <queue> [--count <n>] [--block <ms>]
//!   flo queue watch <queue>              - Continuously watch for messages
//!   flo queue peek <queue> [--count <n>]
//!   flo queue ack <queue> <seq>...
//!   flo queue nack <queue> <seq>...

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const outcome = @import("../outcome.zig");
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
        .subcommand(
            commander.newBuilder(allocator)
                .name("enqueue")
                .about("Add a message to a queue")
                .aliases(&.{"push"})
                .examples(&.{
                    "flo queue enqueue myqueue 'Hello, World!'",
                    "flo queue enqueue tasks '{\"id\":1}' --priority 10",
                })
                .arg("queue", "Queue name")
                .arg("payload", "Message payload")
                .uintFlag("priority", 'p', 0, "Priority (0-255; lower is taken first)")
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
                    "flo queue dequeue myqueue --block 5000",
                })
                .arg("queue", "Queue name")
                .uintFlag("count", 'c', 1, "Number of messages to dequeue")
                .uintFlag("block", 'b', 0, "Block for messages (ms, at most 300000; 0 = don't wait)")
                .action(wrapHandler(runDequeue)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("watch")
                .about("Continuously watch a queue; Ctrl-C ends it (exit 0), a retryable answer exits 4")
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
                .action(wrapHandler(runNack)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("dlq")
                .about("Dead-letter queue operations")
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("list")
                        .about("Show a queue's dead-letter count")
                        .arg("queue", "Queue name")
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
                .action(wrapHandler(runList)),
        )
        .build();
}

fn runEnqueue(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const payload = ctx.getPositional("payload").?; // validated by commander

    const priority_val = ctx.getUint("priority") orelse 0;
    const priority: u8 = if (priority_val > 255) 255 else @intCast(priority_val);
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.queue.enqueue(&client, namespace, queue, payload, priority) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    // The answer is the message's sequence number, which ack and nack take.
    if (result.data.len != 8) return outcome.malformed(ctx, "enqueue answer");
    ctx.print("Enqueued: {d}\n", .{std.mem.readInt(u64, result.data[0..8], .little)});
}

fn runDequeue(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander

    const count = ctx.getUint("count") orelse 1;
    const block = ctx.getChangedUint("block");
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.queue.dequeue(&client, namespace, queue, @intCast(count), if (block) |b| @as(u32, @intCast(b)) else null) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    var messages = Messages.init(result.data) catch return outcome.malformed(ctx, "dequeue answer");
    if (messages.remaining == 0) {
        ctx.print("(no messages)\n", .{});
        return;
    }
    while (messages.next() catch return outcome.malformed(ctx, "dequeue answer")) |payload| {
        ctx.print("{s}\n", .{payload});
    }
}

/// The payloads of a dequeue answer: `[count:u32]` then per message
/// `[seq:u64][payload_len:u32][payload][enqueued_at:i64][delivery_count:u32][priority:u8]`.
/// An empty answer is no messages; one cut short is Truncated.
const Messages = struct {
    reader: WireReader,
    remaining: u32,

    /// Checks every message before returning, so a cut answer prints
    /// nothing rather than the messages before the cut.
    fn init(data: []const u8) error{Truncated}!Messages {
        if (data.len == 0) return .{ .reader = WireReader.init(data), .remaining = 0 };
        var reader = WireReader.init(data);
        const count = reader.readU32() orelse return error.Truncated;
        const first: Messages = .{ .reader = reader, .remaining = count };
        var check = first;
        while (try check.next()) |_| {}
        return first;
    }

    /// What follows the messages (a dead-letter list's total), once the
    /// messages are read.
    fn trailer(self: *const Messages) []const u8 {
        return self.reader.remaining();
    }

    fn next(self: *Messages) error{Truncated}!?[]const u8 {
        if (self.remaining == 0) return null;
        self.remaining -= 1;
        const r = &self.reader;
        _ = r.readU64() orelse return error.Truncated; // seq
        const payload = r.readLengthPrefixed(u32) orelse return error.Truncated;
        _ = r.readI64() orelse return error.Truncated; // enqueued_at
        _ = r.readU32() orelse return error.Truncated; // delivery_count
        _ = r.readU8() orelse return error.Truncated; // priority
        return payload;
    }
};

fn runWatch(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander

    const namespace = cli_config.getNamespace(ctx);
    ctx.print("Watching queue: {s}\n", .{queue});
    ctx.print("Press Ctrl+C to stop.\n\n", .{});

    // Continuous polling loop
    const endpoint = cli_config.getEndpoint(ctx);
    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    outcome.endOnInterrupt();
    while (!outcome.interrupted()) {
        // Block for messages with 1 second timeout
        var result = client_mod.queue.dequeue(&client, namespace, queue, 1, 1000) catch |err| {
            if (outcome.interrupted()) return;
            return outcome.requestFailed(ctx, err);
        };
        defer result.deinit();

        try outcome.check(ctx, result);

        var messages = Messages.init(result.data) catch return outcome.malformed(ctx, "dequeue answer");
        while (messages.next() catch return outcome.malformed(ctx, "dequeue answer")) |payload| {
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

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.queue.peek(&client, namespace, queue, @intCast(count)) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    // A peek answers like a dequeue.
    var messages = Messages.init(result.data) catch return outcome.malformed(ctx, "peek answer");
    if (messages.remaining == 0) {
        ctx.print("(no messages)\n", .{});
        return;
    }
    while (messages.next() catch return outcome.malformed(ctx, "peek answer")) |payload| {
        ctx.print("{s}\n", .{payload});
    }
}

fn runAck(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const seq_str = ctx.getPositional("seq").?; // validated by commander

    const seq = std.fmt.parseInt(u64, seq_str, 10) catch {
        return outcome.usage(ctx, "Invalid sequence number", .{});
    };

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.queue.ack(&client, namespace, queue, &[_]u64{seq}) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("OK\n", .{});
}

fn runNack(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const seq_str = ctx.getPositional("seq").?; // validated by commander

    const seq = std.fmt.parseInt(u64, seq_str, 10) catch {
        return outcome.usage(ctx, "Invalid sequence number", .{});
    };

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.queue.nack(&client, namespace, queue, &[_]u64{seq}) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("OK\n", .{});
}

fn runDlqList(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.queue.dlqList(&client, namespace, queue) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    // Dequeue's layout, then [total_count:u64]: how many are dead-lettered,
    // which a page may not list.
    var messages = Messages.init(result.data) catch return outcome.malformed(ctx, "dead-letter list");
    var listed = messages;
    while (listed.next() catch return outcome.malformed(ctx, "dead-letter list")) |_| {}
    const tail = listed.trailer();
    if (tail.len != 8) return outcome.malformed(ctx, "dead-letter list");
    const total = std.mem.readInt(u64, tail[0..8], .little);
    ctx.print("Dead-letter queue for: {s} ({d} messages)\n", .{ queue, total });
    if (messages.remaining == 0 and total == 0) ctx.print("(empty)\n", .{});
    while (messages.next() catch return outcome.malformed(ctx, "dead-letter list")) |payload| {
        ctx.print("{s}\n", .{payload});
    }
}

fn runDlqRequeue(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const seq_str = ctx.getPositional("seq").?; // validated by commander

    const seq = std.fmt.parseInt(u64, seq_str, 10) catch {
        return outcome.usage(ctx, "Invalid sequence number", .{});
    };

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.queue.dlqRequeue(&client, namespace, queue, &[_]u64{seq}) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

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

fn runList(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const limit = ctx.getUint("limit") orelse 100;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // Accumulate raw row bytes from paginated wire responses.
    // Server sends: [count:u32]([name_len:u32][name][ns_len:u32][ns][pending:u64][available:u64][enqueued:u64][dequeued:u64][dlq:u64])*[has_more:u8][cursor_len:u16][cursor]?
    var row_bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer row_bytes.deinit(ctx.allocator);
    var entry_count: u32 = 0;

    // Track seen names for dedup across shards
    var seen: std.StringHashMapUnmanaged(void) = .{};
    defer {
        var it = seen.keyIterator();
        while (it.next()) |k| ctx.allocator.free(k.*);
        seen.deinit(ctx.allocator);
    }

    var cursor: ?[]const u8 = null;
    var cursor_owned: ?[]u8 = null;
    defer if (cursor_owned) |c| ctx.allocator.free(c);

    while (entry_count < limit) {
        var response = client_mod.queue.list(&client, namespace, @intCast(limit), cursor) catch |err| return outcome.requestFailed(ctx, err);
        defer response.deinit();

        try outcome.check(ctx, response);

        if (response.data.len == 0) break;

        var reader = WireReader.init(response.data);
        const count = reader.readU32() orelse return outcome.malformed(ctx, "queue list");

        var i: u32 = 0;
        while (i < count) : (i += 1) {
            const row_start = reader.pos;

            const name_len = reader.readU32() orelse return outcome.malformed(ctx, "queue list");
            const name = reader.readSlice(name_len) orelse return outcome.malformed(ctx, "queue list");
            const ns_len = reader.readU32() orelse return outcome.malformed(ctx, "queue list");
            _ = reader.readSlice(ns_len) orelse return outcome.malformed(ctx, "queue list"); // namespace
            _ = reader.readU64() orelse return outcome.malformed(ctx, "queue list"); // pending
            _ = reader.readU64() orelse return outcome.malformed(ctx, "queue list"); // available
            _ = reader.readU64() orelse return outcome.malformed(ctx, "queue list"); // enqueued
            _ = reader.readU64() orelse return outcome.malformed(ctx, "queue list"); // dequeued
            _ = reader.readU64() orelse return outcome.malformed(ctx, "queue list"); // dlq

            const row_end = reader.pos;

            if (seen.get(name) != null) continue;
            const name_copy = try ctx.allocator.dupe(u8, name);
            seen.put(ctx.allocator, name_copy, {}) catch |err| {
                ctx.allocator.free(name_copy);
                return err;
            };

            try row_bytes.appendSlice(ctx.allocator, response.data[row_start..row_end]);
            entry_count += 1;
            if (entry_count >= limit) break;
        }

        const has_more = (reader.readU8() orelse return outcome.malformed(ctx, "queue list")) != 0;
        const cursor_len = reader.readU16() orelse return outcome.malformed(ctx, "queue list");
        const next_cursor = if (cursor_len > 0) reader.readSlice(cursor_len) orelse return outcome.malformed(ctx, "queue list") else null;

        if (cursor_owned) |c| ctx.allocator.free(c);
        cursor_owned = null;

        if (!has_more or next_cursor == null) break;

        cursor_owned = try ctx.allocator.dupe(u8, next_cursor.?);
        cursor = cursor_owned;
    }

    if (entry_count == 0) {
        const fmt = output.getFormat(ctx);
        if (fmt == .json) {
            ctx.print("[]\n", .{});
        } else {
            ctx.print("No queues found in namespace '{s}'\n", .{namespace});
        }
        return;
    }

    // Build final wire buffer: [count:u32][accumulated_raw_rows]
    const buf = try ctx.allocator.alloc(u8, 4 + row_bytes.items.len);
    defer ctx.allocator.free(buf);

    std.mem.writeInt(u32, buf[0..4], entry_count, .little);
    @memcpy(buf[4..][0..row_bytes.items.len], row_bytes.items);

    output.printWireList(ctx, buf, "No queues found", &.{
        .{ .field = "name", .header = "NAME", .field_type = .str_u32 },
        .{ .field = "namespace", .header = "NAMESPACE", .field_type = .str_u32 },
        .{ .field = "pending", .header = "PENDING", .field_type = .uint_u64, .alignment = .right },
        .{ .field = "available", .header = "AVAILABLE", .field_type = .uint_u64, .alignment = .right },
        .{ .field = "enqueued", .header = "ENQUEUED", .field_type = .uint_u64, .alignment = .right },
        .{ .field = "dequeued", .header = "DEQUEUED", .field_type = .uint_u64, .alignment = .right },
        .{ .field = "dlq", .header = "DLQ", .field_type = .uint_u64, .alignment = .right },
    }) catch return outcome.malformed(ctx, "list");
}

test "queue: a dequeue answer cut short is Truncated, not fewer messages" {
    // count 2: seq, len 1, "a", enqueued_at, delivery_count, priority; then the same with "b".
    const one = [_]u8{ 1, 0, 0, 0, 0, 0, 0, 0 } ++ [_]u8{ 1, 0, 0, 0 } ++ "a".* ++ [_]u8{0} ** 8 ++ [_]u8{0} ** 4 ++ [_]u8{0};
    const two = one[0..8].* ++ [_]u8{ 1, 0, 0, 0 } ++ "b".* ++ [_]u8{0} ** 13;
    const answer = [_]u8{ 2, 0, 0, 0 } ++ one ++ two;

    var m = try Messages.init(&answer);
    try std.testing.expectEqualStrings("a", (try m.next()).?);
    try std.testing.expectEqualStrings("b", (try m.next()).?);
    try std.testing.expectEqual(@as(?[]const u8, null), try m.next());

    // Checked whole up front: a cut anywhere fails before the first message.
    for (1..answer.len) |n| try std.testing.expectError(error.Truncated, Messages.init(answer[0..n]));
    try std.testing.expectEqual(@as(u32, 0), (try Messages.init("")).remaining);
}

test "queue watch ends with retryable (exit 4) on an overloaded answer, not a retry loop" {
    const allocator = std.testing.allocator;
    const server = try outcome.FakeServer.start(.overloaded, "busy");
    defer server.stop();
    var ep: [32]u8 = undefined;
    const root = try outcome.testRoot(allocator, try createQueueCommand(allocator));
    defer root.deinit();
    const result = root.executeSlice(&.{ "flo", "queue", "watch", "q", "--endpoint", server.endpoint(&ep) });
    try std.testing.expectError(error.Retryable, result);
    try std.testing.expectEqual(@as(u8, 4), outcome.exitCode(result));
}

test "queue enqueue reads its answer as exactly an 8-byte sequence number" {
    const allocator = std.testing.allocator;
    for ([_]struct { body: []const u8, want: ?commander.Error }{
        .{ .body = &[_]u8{ 7, 0, 0, 0, 0, 0, 0, 0 }, .want = null },
        .{ .body = &[_]u8{ 7, 0, 0, 0, 0, 0, 0 }, .want = error.Transport },
        .{ .body = &[_]u8{ 7, 0, 0, 0, 0, 0, 0, 0, 0 }, .want = error.Transport },
    }) |c| {
        const server = try outcome.FakeServer.start(.ok, c.body);
        defer server.stop();
        var ep: [32]u8 = undefined;
        const root = try outcome.testRoot(allocator, try createQueueCommand(allocator));
        defer root.deinit();
        const result = root.executeSlice(&.{ "flo", "queue", "enqueue", "q", "x", "--endpoint", server.endpoint(&ep) });
        if (c.want) |e| try std.testing.expectError(e, result) else try result;
    }
}
