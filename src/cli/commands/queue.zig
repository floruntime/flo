//! Queue commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo queue enqueue <queue> <payload> [--priority <0-255>]
//!   flo queue dequeue <queue> [--count <n>] [--block <ms>] [--lease <ms>] [--consumer <id>] [--ack]
//!   flo queue watch <queue>              - Continuously take and print messages
//!   flo queue peek <queue> [--limit <n>]   - generated from the op table
//!   flo queue ack <queue> <delivery>...
//!   flo queue nack <queue> <delivery>... [--delay <ms>] [--dlq] [--error <text>]
//!   flo queue touch <queue> <delivery>...
//!   flo queue dlq list <queue> [--limit <n>] [--after <cursor>]
//!   flo queue dlq requeue <queue> (--ids <seq,...> | --all [--max <n>])
//!   flo queue dlq delete <queue> (--ids <seq,...> | --all [--max <n>]) [--dry-run]
//!   flo queue purge <queue>             - Remove every message, dead letters too

const std = @import("std");
const Allocator = std.mem.Allocator;
const gen = @import("../gen.zig");
const queue_ops = @import("../../protocol/queue_ops.zig");
const schema = @import("../../protocol/schema.zig");
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
    const queue = commander.newBuilder(allocator)
        .name("queue")
        .about("Queue operations")
        .group("Data Commands")
        .longAbout(
            \\Interact with Flo's message queue system.
            \\
            \\Provides commands for enqueueing, dequeueing, and managing
            \\messages with support for priority and acknowledgements. A dequeue
            \\leases its messages: complete (ack) them within the lease, or they
            \\are delivered again.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("enqueue")
                .about("Add a message to a queue")
                .aliases(&.{"push"})
                .examples(&.{
                    "flo queue enqueue myqueue 'Hello, World!'",
                    "flo queue enqueue tasks '{\"id\":1}' --priority 10",
                    "flo queue enqueue reminders 'ping' --delay 60000",
                })
                .arg("queue", "Queue name")
                .arg("payload", "Message payload")
                .uintFlag("priority", 0, 0, "Priority (0-255; lower is taken first)")
                .uintFlag("delay", 0, 0, "How long before it is ready (ms, at most 86400000)")
                .action(wrapHandler(runEnqueue)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("dequeue")
                .about("Get messages from a queue")
                .aliases(&.{"pop"})
                .examples(&.{
                    "flo queue dequeue myqueue",
                    "flo queue dequeue myqueue --count 10 --lease 60000",
                    "flo queue dequeue myqueue --block 5000",
                    "flo queue dequeue myqueue --ack",
                })
                .arg("queue", "Queue name")
                .uintFlag("count", 'c', 1, "Number of messages to dequeue (at most 100)")
                .uintFlag("block", 'b', 0, "Block for messages (ms, at most 300000; 0 = don't wait)")
                .uintFlag("lease", 0, 0, "How long they're held without a touch (ms, 5000 to 43200000; default 30000)")
                .stringFlag("consumer", 0, "", "Who takes them: ack, nack and touch must name the same")
                .boolFlag("ack", 0, "Complete them as they're taken, printing payloads only")
                .action(wrapHandler(runDequeue)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("watch")
                .about("Continuously take and print a queue's messages, completing each; Ctrl-C ends it (exit 0), a retryable answer exits 4")
                .arg("queue", "Queue name")
                .action(wrapHandler(runWatch)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("ack")
                .about("Complete dequeued messages: they leave the queue")
                .aliases(&.{"complete"})
                .arg("queue", "Queue name")
                .variadicArg("delivery", "Deliveries as dequeue printed them (term.index.seq.deliveries), of one dequeue")
                .stringFlag("consumer", 0, "", "The consumer that dequeued them")
                .action(wrapHandler(runAck)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("nack")
                .about("Fail dequeued messages: ready again, or dead once their attempts are spent")
                .aliases(&.{"fail"})
                .arg("queue", "Queue name")
                .variadicArg("delivery", "Deliveries as dequeue printed them (term.index.seq.deliveries), of one dequeue")
                .stringFlag("consumer", 0, "", "The consumer that dequeued them")
                .uintFlag("delay", 0, 0, "How long before they're ready again (ms)")
                .boolFlag("dlq", 0, "Dead-letter them now")
                .stringFlag("error", 0, "", "Why they failed")
                .action(wrapHandler(runNack)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("touch")
                .about("Keep dequeued messages: with half their lease or less left, it runs a full lease from now")
                .arg("queue", "Queue name")
                .variadicArg("delivery", "Deliveries as dequeue printed them (term.index.seq.deliveries), of one dequeue")
                .stringFlag("consumer", 0, "", "The consumer that dequeued them")
                .action(wrapHandler(runTouch)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("dlq")
                .about("Dead-letter queue operations")
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("list")
                        .about("List a queue's dead letters, oldest dead first")
                        .arg("queue", "Queue name")
                        .uintFlag("limit", 'l', 20, "Most to show (at most 100)")
                        .stringFlag("after", 0, "", "Go on past this one: the cursor a list printed")
                        .action(wrapHandler(runDlqList)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("requeue")
                        .about("Make dead letters ready again, their attempts reset")
                        .arg("queue", "Queue name")
                        .stringFlag("ids", 'i', "", "Comma-separated seqs")
                        .boolFlag("all", 0, "Every dead letter, oldest first, a batch of up to 100 at a time")
                        .uintFlag("max", 0, 0, "With --all, stop after this many (0 for none)")
                        .action(wrapHandler(runDlqRequeue)),
                )
                .subcommand(
                commander.newBuilder(allocator)
                    .name("delete")
                    .about("Delete dead letters for good")
                    .arg("queue", "Queue name")
                    .stringFlag("ids", 'i', "", "Comma-separated seqs")
                    .boolFlag("all", 0, "Every dead letter, oldest first, a batch of up to 100 at a time")
                    .uintFlag("max", 0, 0, "With --all, stop after this many (0 for none)")
                    .boolFlag("dry-run", 0, "Say what it would delete, and delete nothing")
                    .action(wrapHandler(runDlqDelete)),
            ),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("purge")
                .about("Remove every message from a queue, ready, leased and dead")
                .arg("queue", "Queue name")
                .action(wrapHandler(runPurge)),
        )
        .subcommand(
        commander.newBuilder(allocator)
            .name("list")
            .about("List all queues")
            .aliases(&.{"ls"})
            .uintFlag("limit", 'l', 100, "Maximum queues to list")
            .action(wrapHandler(runList)),
    );
    gen.addCommands(allocator, queue, &queue_ops.table, &.{});
    return try queue.build();
}

fn runEnqueue(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const payload = ctx.getPositional("payload").?; // validated by commander

    const priority_val = ctx.getUint("priority") orelse 0;
    if (priority_val > 255) return outcome.usage(ctx, "--priority is 0 to 255, not {d}", .{priority_val});
    const priority: u8 = @intCast(priority_val);
    const delay: u64 = ctx.getUint("delay") orelse 0;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.queue.enqueue(&client, namespace, queue, payload, priority, delay) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    // The answer is the message's sequence number, which ack and nack take.
    if (result.data.len != 8) return outcome.malformed(ctx, "enqueue answer");
    ctx.print("Enqueued: {d}\n", .{std.mem.readInt(u64, result.data[0..8], .little)});
}

fn runDequeue(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander

    const count = ctx.getUint("count") orelse 1;
    if (count < 1 or count > queue_ops.MAX_BATCH) return outcome.usage(ctx, "--count is 1 to {d}, not {d}", .{ queue_ops.MAX_BATCH, count });
    const block = ctx.getChangedUint("block");
    const lease: ?u64 = if (ctx.getChangedUint("lease")) |l| l else null;
    if (lease) |l| if (l < ae.MIN_LEASE_MS or l > ae.MAX_LEASE_MS) return outcome.usage(ctx, "--lease is {d} to {d} ms, not {d}", .{ ae.MIN_LEASE_MS, ae.MAX_LEASE_MS, l });
    const ack = ctx.getBool("ack");
    // Payloads alone carry no delivery, so nothing could ack them: with
    // --ack they're taken for good, without it they'd come back.
    if (output.getFormat(ctx) == .raw and !ack) return outcome.usage(ctx, "-o raw prints payloads only, and nothing could ack them: add --ack to consume them, or use the default or -o json output for their deliveries", .{});
    const consumer = ctx.getString("consumer") orelse "";
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.queue.dequeue(&client, namespace, queue, count, if (block) |b| @as(u32, @intCast(b)) else null, ack, consumer, lease) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);
    try printMessages(ctx, result.data, ack, true);
}

/// A dequeue's messages, one a line: with `ack` its payload, else its
/// delivery (which ack, nack and touch take) then its payload. `-o json`
/// prints the answer as an object, each message with its delivery.
fn printMessages(ctx: *commander.Context, data: []const u8, ack: bool, say_none: bool) commander.Error!void {
    var diag: @import("../../protocol/schema.zig").Diagnostic = .{};
    const answer = DequeueResponse.decodeAll(data, &diag) catch return outcome.malformed(ctx, "dequeue answer");
    if (output.getFormat(ctx) == .json) return printJson(ctx, answer);
    if (answer.messages.count == 0) {
        if (say_none) ctx.print("(no messages)\n", .{});
        return;
    }
    var it = answer.messages.iterator();
    while (it.next() catch return outcome.malformed(ctx, "dequeue answer")) |m| {
        if (ack) {
            ctx.print("{s}\n", .{m.payload});
        } else {
            const id: client_mod.queue.DeliveryId = .{ .term = answer.term, .index = answer.index, .seq = m.seq, .deliveries = m.deliveries };
            ctx.print("{f} {s}\n", .{ id, m.payload });
        }
    }
}

const DequeueResponse = queue_ops.op("queue.dequeue").response;
const ae = @import("../../protocol/action_entries.zig");

fn printJson(ctx: *commander.Context, answer: DequeueResponse.Value) commander.Error!void {
    ctx.print("{{\"term\":{d},\"index\":{d},\"expires_at_ms\":{d},\"messages\":[", .{ answer.term, answer.index, answer.expires_at_ms });
    var it = answer.messages.iterator();
    var first = true;
    while (it.next() catch return outcome.malformed(ctx, "dequeue answer")) |m| {
        const id: client_mod.queue.DeliveryId = .{ .term = answer.term, .index = answer.index, .seq = m.seq, .deliveries = m.deliveries };
        ctx.print("{s}{{\"delivery\":\"{f}\",\"seq\":{d},\"priority\":{d},\"deliveries\":{d},\"enqueued_at_ms\":{d},\"payload\":{f}}}", .{ if (first) "" else ",", id, m.seq, m.priority, m.deliveries, m.enqueued_at_ms, output.jsonString(m.payload) });
        first = false;
    }
    ctx.print("]}}\n", .{});
}
const ReportResponse = queue_ops.op("queue.complete").response;
const TouchResponse = queue_ops.op("queue.touch").response;

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
        var result = client_mod.queue.dequeue(&client, namespace, queue, 1, 1000, true, "", null) catch |err| {
            if (outcome.interrupted()) return;
            return outcome.requestFailed(ctx, err);
        };
        defer result.deinit();

        try outcome.check(ctx, result);
        try printMessages(ctx, result.data, true, false);
    }
}

/// The deliveries a report names, from its arguments.
fn deliveries(ctx: *commander.Context, buf: []client_mod.queue.DeliveryId) commander.Error![]const client_mod.queue.DeliveryId {
    const args = ctx.getVariadicArgs("delivery") orelse &.{};
    if (args.len == 0) return outcome.usage(ctx, "name at least one delivery, as dequeue printed it (term.index.seq.deliveries)", .{});
    if (args.len > buf.len) return outcome.usage(ctx, "at most {d} deliveries at a time", .{buf.len});
    for (args, 0..) |a, i| {
        buf[i] = client_mod.queue.DeliveryId.parse(a) orelse return outcome.usage(ctx, "'{s}' isn't a delivery: dequeue prints them as term.index.seq.deliveries", .{a});
        if (buf[i].term != buf[0].term or buf[i].index != buf[0].index) return outcome.usage(ctx, "deliveries of one dequeue at a time: '{s}' is another's", .{a});
    }
    return buf[0..args.len];
}

/// A report's refusals, said one a line; any refuses the command.
fn printRefused(ctx: *commander.Context, refused: @import("../../protocol/schema.zig").List(queue_ops.Refused, queue_ops.MAX_BATCH).Value) commander.Error!void {
    if (refused.count == 0) return;
    var it = refused.iterator();
    while (it.next() catch return outcome.malformed(ctx, "report answer")) |r| ctx.printErr("Error: seq {d}: {s}\n", .{ r.seq, r.reason });
    return error.Refused;
}

const Report = enum { ack, nack, touch };

fn runReport(ctx: *commander.Context, comptime kind: Report) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    var buf: [queue_ops.MAX_BATCH]client_mod.queue.DeliveryId = undefined;
    const ids = try deliveries(ctx, &buf);
    const consumer = ctx.getString("consumer") orelse "";
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = switch (kind) {
        .ack => client_mod.queue.complete(&client, namespace, queue, consumer, ids),
        .nack => client_mod.queue.fail(&client, namespace, queue, consumer, ids, ctx.getUint("delay") orelse 0, ctx.getBool("dlq"), ctx.getString("error") orelse ""),
        .touch => client_mod.queue.touch(&client, namespace, queue, consumer, ids),
    } catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);
    var diag: @import("../../protocol/schema.zig").Diagnostic = .{};
    if (kind == .touch) {
        const answer = TouchResponse.decodeAll(result.data, &diag) catch return outcome.malformed(ctx, "touch answer");
        var tb: [64]u8 = undefined;
        ctx.print("OK: {d} held until {s}\n", .{ answer.applied, untilText(&tb, answer.expires_at_ms) });
        return printRefused(ctx, answer.refused);
    }
    const answer = ReportResponse.decodeAll(result.data, &diag) catch return outcome.malformed(ctx, "report answer");
    ctx.print("OK: {d}\n", .{answer.applied});
    return printRefused(ctx, answer.refused);
}

/// When a lease runs out, as a person reads it: UTC and how far off.
fn untilText(buf: []u8, at_ms: i64) []const u8 {
    if (at_ms <= 0) return "(not held)";
    const secs: u64 = @intCast(@divFloor(at_ms, 1000));
    const es = std.time.epoch.EpochSeconds{ .secs = secs };
    const day = es.getEpochDay().calculateYearDay();
    const md = day.calculateMonthDay();
    const ds = es.getDaySeconds();
    const now_ms = @import("stdx").time.milliTimestamp();
    const left = @divFloor(at_ms - now_ms, 1000);
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2} UTC ({d} s)", .{ day.year, md.month.numeric(), md.day_index + 1, ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute(), left }) catch "?";
}

fn runAck(ctx: *commander.Context) commander.Error!void {
    return runReport(ctx, .ack);
}

fn runNack(ctx: *commander.Context) commander.Error!void {
    return runReport(ctx, .nack);
}

fn runTouch(ctx: *commander.Context) commander.Error!void {
    return runReport(ctx, .touch);
}

fn runDlqList(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?;
    const limit = ctx.getUint("limit") orelse 20;
    if (limit == 0 or limit > queue_ops.MAX_DLQ_PAGE) return outcome.usage(ctx, "--limit is 1 to {d}", .{queue_ops.MAX_DLQ_PAGE});
    const after_str = ctx.getString("after") orelse "";
    const after: ?queue_ops.DeadCursor.Value = if (after_str.len == 0) null else parseCursor(after_str) orelse
        return outcome.usage(ctx, "--after '{s}' isn't a cursor: give dead_ns.seq, as a list printed it", .{after_str});
    var client = Client.init(ctx.allocator, cli_config.getEndpoint(ctx));
    defer client.deinit();
    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);
    var response = client_mod.queue.dlqList(&client, cli_config.getNamespace(ctx), queue, @intCast(limit), after) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();
    try outcome.check(ctx, response);
    var diag: schema.Diagnostic = .{};
    const a = queue_ops.op("queue.dlq_list").response.decodeAll(response.data, &diag) catch return outcome.malformed(ctx, "queue.dlq_list answer");
    const json_output = output.getFormat(ctx) == .json;
    if (json_output) ctx.print("{{\"dead\":[", .{});
    var it = a.dead.iterator();
    var i: usize = 0;
    while (it.next() catch null) |d| : (i += 1) {
        if (json_output) {
            ctx.print("{s}{{\"seq\":{d},\"priority\":{d},\"deliveries\":{d},\"failures\":{d},\"enqueued_at_ms\":{d},\"dead_at_ms\":{d},\"size\":{d},\"payload\":{f},\"error\":{f}}}", .{ if (i == 0) "" else ",", d.seq, d.priority, d.deliveries, d.failures, d.enqueued_at_ms, d.dead_at_ms, d.size, output.jsonString(d.payload), output.jsonString(d.@"error") });
        } else ctx.print("{d}\tdeliveries={d} failures={d} dead_at_ms={d}\t{s}{s}\t{s}\n", .{ d.seq, d.deliveries, d.failures, d.dead_at_ms, d.payload, if (d.size > d.payload.len) "…" else "", d.@"error" });
    }
    if (json_output) {
        if (a.cursor) |c| ctx.print("],\"cursor\":\"{d}.{d}\"}}\n", .{ c.dead_ns, c.seq }) else ctx.print("],\"cursor\":null}}\n", .{});
    } else if (a.cursor) |c| ctx.printErr("Note: more dead letters follow: --after {d}.{d}\n", .{ c.dead_ns, c.seq });
    if (!json_output and a.dead.count == 0) ctx.print("No dead letters\n", .{});
}

fn parseCursor(text: []const u8) ?queue_ops.DeadCursor.Value {
    const dot = std.mem.indexOfScalar(u8, text, '.') orelse return null;
    const dead_ns = std.fmt.parseInt(u64, text[0..dot], 10) catch return null;
    const seq = std.fmt.parseInt(u64, text[dot + 1 ..], 10) catch return null;
    return .{ .dead_ns = dead_ns, .seq = seq };
}

fn runDlqRequeue(ctx: *commander.Context) commander.Error!void {
    return runDlq(ctx, .requeue);
}

fn runDlqDelete(ctx: *commander.Context) commander.Error!void {
    return runDlq(ctx, .delete);
}

/// Each batch is said as it is answered: how many it applied to, and each
/// message it refused, so a run cut short shows how far it got.
fn runDlq(ctx: *commander.Context, comptime kind: enum { requeue, delete }) commander.Error!void {
    const queue = ctx.getPositional("queue").?;
    const all = ctx.getBool("all");
    const max = ctx.getUint("max") orelse 0;
    const dry_run = kind == .delete and ctx.getBool("dry-run");
    const ids_str = ctx.getString("ids") orelse "";
    if (all == (ids_str.len > 0)) return outcome.usage(ctx, "name the dead letters with --ids, or give --all", .{});
    if (max > 0 and !all) return outcome.usage(ctx, "--max goes with --all", .{});
    var buf: [queue_ops.MAX_BATCH * 9]u8 = undefined;
    var w: schema.Writer = .{ .buf = &buf };
    var n: u32 = 0;
    var iter = std.mem.splitScalar(u8, ids_str, ',');
    while (iter.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " ");
        if (trimmed.len == 0) continue;
        const seq = std.fmt.parseInt(u64, trimmed, 10) catch return outcome.usage(ctx, "'{s}' isn't a seq: give one as a dlq list printed it", .{trimmed});
        if (n == queue_ops.MAX_BATCH) return outcome.usage(ctx, "--ids names at most 100 messages", .{});
        schema.Int(u64).encode(&w, seq) catch return outcome.usage(ctx, "--ids is too long", .{});
        n += 1;
    }
    var client = Client.init(ctx.allocator, cli_config.getEndpoint(ctx));
    defer client.deinit();
    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);
    const namespace = cli_config.getNamespace(ctx);
    const verb = if (kind == .requeue) "Requeued" else if (dry_run) "Would delete" else "Deleted";
    var total: u64 = 0;
    var refused_total: u64 = 0;
    var batch: u32 = 0;
    while (true) : (batch += 1) {
        const messages: ?client_mod.queue.Seqs.Value = if (all) null else .{ .bytes = buf[0..w.pos], .count = n };
        // No batch takes more than --max leaves.
        const limit: u32 = if (max > 0) @intCast(@min(queue_ops.MAX_BATCH, max - total)) else queue_ops.MAX_BATCH;
        var response = switch (kind) {
            .requeue => client.call(queue_ops.op("queue.dlq_requeue"), namespace, queue, .{ .messages = messages, .all = all, .limit = limit }),
            .delete => client.call(queue_ops.op("queue.dlq_delete"), namespace, queue, .{ .messages = messages, .all = all, .limit = limit, .dry_run = dry_run }),
        } catch |err| return outcome.requestFailed(ctx, err);
        defer response.deinit();
        try outcome.check(ctx, response);
        var diag: schema.Diagnostic = .{};
        const a = queue_ops.op("queue.dlq_requeue").response.decodeAll(response.data, &diag) catch return outcome.malformed(ctx, "dead-letter answer");
        if (all) ctx.print("batch {d}: ", .{batch + 1});
        // A dry run looks at one batch: past it, more may remain.
        ctx.print("{s} {d} message(s){s}\n", .{ verb, a.applied, if (dry_run and a.more) ", and more remain" else "" });
        var rit = a.refused.iterator();
        while (rit.next() catch null) |r| ctx.printErr("  seq {d}: {s}\n", .{ r.seq, r.reason });
        total += a.applied;
        refused_total += a.refused.count;
        // A dry run changes nothing, so its first batch is the whole answer;
        // a batch that applied to none would come back the same.
        if (!all or !a.more or dry_run or a.applied == 0) break;
        if (max > 0 and total >= max) break;
    }
    if (all and batch > 0) ctx.print("{s} {d} message(s) in {d} batches\n", .{ verb, total, batch + 1 });
    if (refused_total > 0) {
        ctx.printErr("Error: {d} message(s) not {s}\n", .{ refused_total, if (kind == .requeue) "requeued" else "deleted" });
        return error.Refused;
    }
}

fn runPurge(ctx: *commander.Context) commander.Error!void {
    const queue = ctx.getPositional("queue").?; // validated by commander
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.queue.purge(&client, namespace, queue) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    if (result.data.len != 4) return outcome.malformed(ctx, "purge answer");
    ctx.print("Purged: {d}\n", .{std.mem.readInt(u32, result.data[0..4], .little)});
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
            _ = reader.readU64() orelse return outcome.malformed(ctx, "queue list"); // ready
            _ = reader.readU64() orelse return outcome.malformed(ctx, "queue list"); // leased
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
        .{ .field = "ready", .header = "READY", .field_type = .uint_u64, .alignment = .right },
        .{ .field = "leased", .header = "LEASED", .field_type = .uint_u64, .alignment = .right },
        .{ .field = "enqueued", .header = "ENQUEUED", .field_type = .uint_u64, .alignment = .right },
        .{ .field = "dequeued", .header = "DELIVERED", .field_type = .uint_u64, .alignment = .right },
        .{ .field = "dlq", .header = "DLQ", .field_type = .uint_u64, .alignment = .right },
    }) catch return outcome.malformed(ctx, "list");
}

test "queue: a delivery prints as it parses, and anything else is refused" {
    const id: client_mod.queue.DeliveryId = .{ .term = 3, .index = 17, .seq = 5, .deliveries = 2 };
    var buf: [64]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "{f}", .{id});
    try std.testing.expectEqualStrings("3.17.5.2", text);
    try std.testing.expectEqual(id, client_mod.queue.DeliveryId.parse(text).?);
    for ([_][]const u8{ "", "5", "3.17.5", "3.17.5.2.1", "a.b.c.d", "3.17.5.-1" }) |bad|
        try std.testing.expect(client_mod.queue.DeliveryId.parse(bad) == null);
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

test "an answer that fails its checksum or claims too much ends the command with transport (exit 5)" {
    const allocator = std.testing.allocator;
    const ResponseHeader = @import("../../protocol/proto.zig").ResponseHeader;
    for ([_]struct { offset: usize, want: []const u8 }{
        .{ .offset = @sizeOf(ResponseHeader), .want = "Error: the answer from the server failed its checksum\n" },
        .{ .offset = @offsetOf(ResponseHeader, "data_len") + 3, .want = "Error: the server's answer claims 16777224 bytes, more than an answer can hold\n" },
    }) |c| {
        const server = try outcome.FakeServer.startCorrupt(.ok, &[_]u8{ 7, 0, 0, 0, 0, 0, 0, 0 }, c.offset);
        defer server.stop();
        var ep: [32]u8 = undefined;
        var fds: [2]std.posix.fd_t = undefined;
        if (std.c.pipe(&fds) != 0) return error.NoPipe;
        defer _ = std.c.close(fds[0]);
        const root = try outcome.testRootTo(allocator, try createQueueCommand(allocator), fds[1]);
        defer root.deinit();
        const result = root.executeSlice(&.{ "flo", "queue", "enqueue", "q", "x", "--endpoint", server.endpoint(&ep) });
        _ = std.c.close(fds[1]);
        var out: [256]u8 = undefined;
        var n: usize = 0;
        while (true) {
            const got = std.c.read(fds[0], out[n..].ptr, out.len - n);
            if (got <= 0) break;
            n += @intCast(got);
        }
        try std.testing.expectError(error.Transport, result);
        try std.testing.expectEqual(@as(u8, 5), outcome.exitCode(result));
        try std.testing.expectEqualStrings(c.want, out[0..n]);
    }
}
