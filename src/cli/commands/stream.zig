//! Stream commands for Flo CLI using Commander framework
//!
//! StreamID Format: `<timestamp_ms>-<sequence>` (e.g., "1703350800000-0")
//!   - timestamp_ms: Unix milliseconds since epoch
//!   - sequence: Counter for multiple entries in same millisecond
//!   - Special values: "0" or "0-0" (beginning), "$" (latest/tail)
//!
//! Usage:
//!   flo stream create <stream> [--partitions N] [--retention N] [--retention-count N]
//!   flo stream alter <stream> [--retention N] [--retention-count N]
//!   flo stream append <stream> <payload>... [--header <k=v,k=v,...>]
//!   flo stream read <stream> [--start <streamid>] [--limit <n>] [--follow] [--block <ms>]
//!   flo stream info <stream>
//!   flo stream trim <stream> [--before <streamid>] [--maxlen <n>]
//!   flo stream list
//!
//! Consumer Group Commands:
//!   flo stream group read <stream> --group <name> --consumer <id>
//!   flo stream group ack <stream> --group <name> --ids <id1,id2,...>
//!   flo stream group info <stream> --group <name>

const std = @import("std");
const stdx = @import("stdx");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const outcome = @import("../outcome.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const wire = @import("../../util/wire.zig");
const StreamID = @import("../../stream/stream_id.zig").StreamID;
const proto = @import("../../protocol/proto.zig");
const MAX_BATCH_RECORDS = proto.MAX_STREAM_BATCH_RECORDS;
const output = @import("../output.zig");
const cli_config = @import("../config.zig");
const gen = @import("../gen.zig");
const queue_ops = @import("../../protocol/queue_ops.zig");
const stream_ops = @import("../../protocol/stream_ops.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the stream command tree
pub fn createStreamCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("stream")
        .about("Stream operations")
        .group("Data Commands")
        .longAbout(
            \\Interact with Flo's append-only log streams.
            \\
            \\Streams are ordered, immutable sequences of records. They support
            \\consumer groups for distributed processing with at-least-once delivery.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("append")
                .about("Append records to a stream")
                .aliases(&.{"add"})
                .examples(&.{
                    "flo stream append events 'Hello, World!'",
                    "flo stream append logs '{\"level\":\"info\"}' --header source=web,version=2",
                    "flo stream append metrics value1 value2 value3",
                })
                .arg("stream", "Stream name")
                .variadicArg("payloads", "Record payload(s) - can specify multiple")
                .stringFlag("header", 'H', "", "Headers as key=value pairs, comma-separated")
                .uintFlag("partition", 0, 0, "Target partition")
                .stringFlag("partition-key", 'k', "", "Partition key for routing")
                .action(wrapHandler(runAppend)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("read")
                .about("Read records from a stream")
                .aliases(&.{"get"})
                .examples(&.{
                    "flo stream read events",
                    "flo stream read events --start 1703350800000-0 --limit 50",
                    "flo stream read events --start 0-0 --end 1703350900000-0",
                    "flo stream read events --start 0-0 --follow",
                    "flo stream read logs --output json",
                })
                .arg("stream", "Stream name")
                .stringFlag("start", 's', "0-0", "Read records after this StreamID (exclusive; timestamp-sequence, 0-0=beginning, $=latest)")
                .stringFlag("end", 0, "", "Ending StreamID (timestamp-sequence, inclusive)")
                .uintFlag("limit", 'l', 10, "Maximum records to read; appends are returned whole, so one larger append comes back entire")
                .boolFlag("follow", 'f', "Keep reading new records (like tail -f); Ctrl-C ends it (exit 0), a retryable answer exits 4")
                .uintFlag("block", 'b', 0, "Block for new data (ms, at most 300000; 0 = don't wait). Single read unlike --follow.")
                .uintFlag("partition", 'P', 0, "Read only this partition (default: every partition)")
                .stringFlag("partition-key", 'k', "", "Partition key for routing (reads from same partition as append)")
                .action(wrapHandler(runRead)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("create")
                .about("Create a new stream")
                .examples(&.{
                    "flo stream create events",
                    "flo stream create logs --partitions 4",
                })
                .arg("stream", "Stream name")
                .uintFlag("partitions", 0, 1, "Number of partitions")
                .uintFlag("retention", 'r', 0, "Retention period (hours, 0=forever)")
                .uint64Flag("retention-count", 0, 0, "Keep at most this many records (0 = no count bound)")
                .action(wrapHandler(runCreate)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("info")
                .about("Show stream information")
                .examples(&.{
                    "flo stream info events",
                    "flo stream info events --output json",
                })
                .arg("stream", "Stream name")
                .action(wrapHandler(runInfo)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List all streams")
                .aliases(&.{"ls"})
                .uintFlag("limit", 'l', 100, "Maximum streams to list")
                .action(wrapHandler(runList)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("trim")
                .about("Trim stream records")
                .examples(&.{
                    "flo stream trim events --before 1703350800000-0",
                    "flo stream trim logs --maxlen 1000",
                })
                .arg("stream", "Stream name")
                .stringFlag("before", 0, "", "Remove records before this StreamID (timestamp-sequence)")
                .uint64Flag("maxlen", 0, 0, "Keep only this many records")
                .uint64Flag("maxage", 0, 0, "Remove records older than this (seconds)")
                .boolFlag("dry-run", 'd', "Show what would be trimmed without trimming")
                .action(wrapHandler(runTrim)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("alter")
                .about("Alter stream configuration")
                .examples(&.{
                    "flo stream alter events --retention 24",
                    "flo stream alter logs --retention 168",
                    "flo stream alter audit --retention-count 100000",
                })
                .arg("stream", "Stream name")
                .uintFlag("retention", 'r', 0, "Keep records this many hours (0 keeps the current bound)")
                .uint64Flag("retention-count", 0, 0, "Keep at most this many records (0 keeps the current bound)")
                .action(wrapHandler(runAlter)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("delete")
                .about("Delete a stream and all its data")
                .aliases(&.{"rm"})
                .examples(&.{
                    "flo stream delete events",
                    "flo stream delete events --force",
                })
                .arg("stream", "Stream name")
                .boolFlag("force", 'f', "Delete even if the stream is not empty")
                .action(wrapHandler(runDelete)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("group")
                .about("Consumer group operations")
                .aliases(&.{"cg"})
                .subcommand(gen.command(allocator, stream_ops.op("stream.group.list")))
                .subcommand(gen.command(allocator, stream_ops.op("stream.group.members")))
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("read")
                        .about("Read from consumer group")
                        .examples(&.{
                            "flo stream group read events --group mygroup --consumer worker1",
                            "flo stream group read events --group mygroup --consumer worker1 --limit 10",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("consumer", 'c', "", "Consumer ID (required)")
                        .uintFlag("limit", 'l', 1, "Maximum records to lease, each on its own (at most 100 a read; more reads 100)")
                        .uintFlag("block", 'b', 0, "Block for new data (ms, at most 300000; 0 = don't wait)")
                        .action(wrapHandler(runGroupRead)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("ack")
                        .about("Acknowledge records by the delivery a read gave each")
                        .examples(&.{
                            "flo stream group ack events --group mygroup --consumer worker1 --ids 3.42.1703350800000-0.1",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("consumer", 'c', "", "The consumer that read them")
                        .stringFlag("ids", 'i', "", "Comma-separated deliveries, as a read printed them: term.index.id.deliveries")
                        .action(wrapHandler(runGroupAck)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("info")
                        .about("Show consumer group information")
                        .examples(&.{
                            "flo stream group info events --group mygroup",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .action(wrapHandler(runGroupInfo)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("pending")
                        .about("Show pending messages in a consumer group")
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("consumer", 'c', "", "Filter to a single consumer's pending entries")
                        .action(wrapHandler(runGroupPending)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("nack")
                        .about("Fail records: ready again after --delay, or dead once their attempts are spent")
                        .examples(&.{
                            "flo stream group nack events --group mygroup --consumer worker1 --ids 3.42.1703350800000-0.1 --delay 10000 --error \"upstream timed out\"",
                            "flo stream group nack events --group mygroup --consumer worker1 --ids 3.42.1703350800000-0.1 --dlq",
                            "flo stream group nack events --group mygroup --consumer worker1 --ids 3.42.1703350800000-0.1 --abandon",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("consumer", 'c', "", "The consumer that read them")
                        .stringFlag("ids", 'i', "", "Comma-separated deliveries, as a read printed them: term.index.id.deliveries")
                        .uintFlag("delay", 0, 0, "How long before they're ready again (ms, at most a day)")
                        .boolFlag("dlq", 0, "Dead-letter them now, whatever attempts are left")
                        .stringFlag("error", 0, "", "Why they failed")
                        .boolFlag("abandon", 0, "Hand them back unstarted: ready again at once, no attempt spent (alone: not with --delay, --dlq or --error)")
                        .action(wrapHandler(runGroupNack)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("touch")
                        .about("Keep records: with half their lease or less left, a full lease from now")
                        .examples(&.{
                            "flo stream group touch events --group mygroup --consumer worker1 --ids 3.42.1703350800000-0.1",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("consumer", 'c', "", "The consumer that read them")
                        .stringFlag("ids", 'i', "", "Comma-separated deliveries, as a read printed them: term.index.id.deliveries")
                        .action(wrapHandler(runGroupTouch)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("create")
                        .about("Create a consumer group with configuration")
                        .longAbout(
                            \\Create a consumer group. Its consumers compete for records: each
                            \\record goes to one of them, leased until acked. --ack-timeout is the
                            \\lease: a record not acked or touched that long is delivered again.
                            \\--max-deliver is the attempts: a record nacked or expired that many
                            \\times is dead.
                        )
                        .examples(&.{
                            "flo stream group create events --group processors",
                            "flo stream group create events --group reliable --ack-timeout 60000 --max-deliver 5",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .uintFlag("ack-timeout", 0, 30000, "The lease (ms, 5000 to 43200000): a record not acked this long is delivered again")
                        .uintFlag("max-deliver", 0, 5, "The attempts (1 to 255): a record nacked or expired this many times is dead")
                        .action(wrapHandler(runGroupCreate)),
                )
                .subcommand(
                commander.newBuilder(allocator)
                    .name("delete")
                    .about("Delete a consumer group and all its state")
                    .longAbout(
                        \\Delete a consumer group: its settings, cursor, leases and dead
                        \\records. It is gone at once; its records are cleaned out after.
                        \\
                        \\This is a destructive operation and cannot be undone.
                    )
                    .examples(&.{
                        "flo stream group delete events --group processors",
                        "flo stream group delete events --group mygroup --output json",
                    })
                    .arg("stream", "Stream name")
                    .stringFlag("group", 'g', "", "Consumer group name (required)")
                    .action(wrapHandler(runGroupDelete)),
            ),
        )
        .build();
}

/// A client call that failed with `error.ServerError`: the client kept the
/// refusal's message and status.
fn serverRefused(ctx: *commander.Context, client: *Client) commander.Error {
    return outcome.refusal(ctx, client.server_error_status, client.server_error_reason, client.serverError(), "", .{});
}

fn runAppend(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander
    const payloads = ctx.getVariadicArgs("payloads") orelse &[_][]const u8{};

    if (payloads.len == 0) {
        ctx.printErr("Error: at least one payload is required\n", .{});
        return error.MissingRequiredArg;
    }
    if (payloads.len > MAX_BATCH_RECORDS) {
        return outcome.usage(ctx, "a batch of {d} records is over the limit of {d}", .{ payloads.len, MAX_BATCH_RECORDS });
    }

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;
    const partition_opt = ctx.getChangedUint("partition");
    const pk_raw = ctx.getString("partition-key");
    const partition_key: ?[]const u8 = if (pk_raw) |pk| (if (pk.len > 0) pk else null) else null;

    // Parse --header "key1=val1,key2=val2" into Header slice
    const Header = client_mod.stream.Header;
    var parsed_headers: [64]Header = undefined;
    var header_count: usize = 0;
    if (ctx.getString("header")) |hdr_str| {
        if (hdr_str.len > 0) {
            var it = std.mem.splitScalar(u8, hdr_str, ',');
            while (it.next()) |pair| {
                const trimmed = std.mem.trim(u8, pair, " ");
                if (trimmed.len == 0) continue;
                if (std.mem.indexOfScalar(u8, trimmed, '=')) |eq| {
                    if (header_count >= 64) {
                        return outcome.usage(ctx, "too many headers (max 64)", .{});
                    }
                    parsed_headers[header_count] = .{
                        .key = trimmed[0..eq],
                        .value = trimmed[eq + 1 ..],
                    };
                    header_count += 1;
                } else {
                    return outcome.usage(ctx, "invalid header format '{s}', expected key=value", .{trimmed});
                }
            }
        }
    }

    // Apply the same headers to every payload in the batch
    const rec_headers: []const Header = parsed_headers[0..header_count];
    var per_payload_headers: [MAX_BATCH_RECORDS][]const Header = undefined;
    for (0..payloads.len) |i| {
        per_payload_headers[i] = rec_headers;
    }
    const headers_arg: ?[]const []const Header = if (header_count > 0)
        per_payload_headers[0..payloads.len]
    else
        null;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    const result = client_mod.stream.appendEx(
        &client,
        namespace,
        stream,
        payloads,
        headers_arg,
        partition_key,
        if (partition_opt) |p| @intCast(p) else null,
    ) catch |err| {
        if (err == error.ServerError) return serverRefused(ctx, &client);
        return outcome.requestFailed(ctx, err);
    };

    // Construct StreamID from the append response
    const stream_id = result.first_id;
    var id_buf: [64]u8 = undefined;
    const id_str = stream_id.format(&id_buf) catch "0-0";

    if (json_output) {
        if (payloads.len == 1) {
            ctx.print("{{\"stream_id\":{f},\"count\":1}}\n", .{output.jsonString(id_str)});
        } else {
            const last_id = result.last_id;
            var last_buf: [64]u8 = undefined;
            const last_str = last_id.format(&last_buf) catch "0-0";
            ctx.print("{{\"first_id\":{f},\"last_id\":{f},\"count\":{d}}}\n", .{ output.jsonString(id_str), output.jsonString(last_str), payloads.len });
        }
    } else {
        if (payloads.len == 1) {
            ctx.print("Appended at {s}\n", .{id_str});
        } else {
            ctx.print("Appended {d} records starting at {s}\n", .{ payloads.len, id_str });
        }
    }
}

fn runRead(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander

    const start_str = ctx.getString("start") orelse "0-0";
    const end_str = ctx.getString("end");
    const limit = ctx.getUint("limit") orelse 10;
    const follow = ctx.getBool("follow");
    const block = ctx.getChangedUint("block");
    const read_partition = ctx.getChangedUint("partition");
    const pk_raw = ctx.getString("partition-key");
    const partition_key: ?[]const u8 = if (pk_raw) |pk| (if (pk.len > 0) pk else null) else null;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    // Parse StreamID from start string
    var current_start = StreamID.parse(start_str) catch {
        return outcome.usage(ctx, "Invalid StreamID format '{s}'. Use '<timestamp>-<sequence>' (e.g., '1703350800000-0')", .{start_str});
    };

    // Parse optional end StreamID
    const end_id: ?StreamID = if (end_str) |es| blk: {
        if (es.len == 0) break :blk null;
        break :blk StreamID.parse(es) catch {
            return outcome.usage(ctx, "Invalid end StreamID format '{s}'. Use '<timestamp>-<sequence>' (e.g., '1703350800000-0')", .{es});
        };
    } else null;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // Follow mode: continuous blocking reads with auto-advancing start position.
    // --follow implies --block 5000 (5 second polling interval).
    // --block N: single blocking read with N ms timeout.
    // Neither: single non-blocking read.
    const block_ms: ?u32 = if (follow) 5000 else if (block) |b| @intCast(b) else null;

    // For --follow mode, loop continuously until Ctrl-C; otherwise single read.
    if (follow) outcome.endOnInterrupt();
    var first_batch = true;
    while (true) {
        const start_mode: client_mod.stream.StartMode = if (current_start.eql(StreamID.MAX)) .tail else .stream_id;

        var response = client_mod.stream.read(&client, namespace, stream, start_mode, current_start, end_id, @intCast(limit), block_ms, if (read_partition) |rp| @intCast(rp) else null, partition_key) catch |err| {
            if (follow and outcome.interrupted()) return;
            return outcome.requestFailed(ctx, err);
        };
        defer response.deinit();

        try outcome.check(ctx, response);

        _ = parseAndPrintRecords(null, &response, json_output, first_batch, follow) catch
            return outcome.malformed(ctx, "stream records");
        const result = parseAndPrintRecords(ctx, &response, json_output, first_batch, follow) catch
            return outcome.malformed(ctx, "stream records");
        first_batch = false;

        if (result.last_id) |last_id| {
            // The start is exclusive: the last record received is the cursor.
            current_start = last_id;
        }

        if (!follow or outcome.interrupted()) break;
    }
}

/// Result from parsing and printing stream records
const ParseResult = struct {
    count: u32,
    last_id: ?StreamID,
};

/// Parse response wire format and print records. Returns the last StreamID seen.
/// With no `out`, it only checks the answer: the caller checks first and
/// then prints, so a cut answer prints no partial records.
fn parseAndPrintRecords(
    out: ?*commander.Context,
    response: *client_mod.Response,
    json_output: bool,
    first_batch: bool,
    is_follow: bool,
) error{Truncated}!ParseResult {
    const ctx: struct {
        c: ?*commander.Context,
        fn print(self: @This(), comptime fmt: []const u8, args: anytype) void {
            if (self.c) |c| c.print(fmt, args);
        }
    } = .{ .c = out };
    _ = first_batch;
    // An empty answer is no records; anything shorter than a count is cut.
    if (response.data.len == 0) {
        if (!is_follow) {
            if (json_output) {
                ctx.print("[]\n", .{});
            } else {
                ctx.print("No records\n", .{});
            }
        }
        return .{ .count = 0, .last_id = null };
    }

    var reader = wire.WireReader.init(response.data);
    const count = reader.readU32() orelse return error.Truncated;

    if (count == 0) {
        if (!is_follow) {
            if (json_output) {
                ctx.print("[]\n", .{});
            } else {
                ctx.print("No records\n", .{});
            }
        }
        return .{ .count = 0, .last_id = null };
    }

    if (json_output and !is_follow) {
        ctx.print("[", .{});
    }

    var last_id: ?StreamID = null;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const msg_sequence = reader.readU64() orelse return error.Truncated;
        const timestamp_ms = reader.readI64() orelse return error.Truncated;
        const tier = reader.readU8() orelse return error.Truncated;
        _ = reader.readU32() orelse return error.Truncated; // partition
        const key_present = reader.readU8() orelse return error.Truncated;
        if (key_present != 0) {
            // Skip key if present
            _ = reader.readLengthPrefixed(u32) orelse return error.Truncated;
        }
        const payload = reader.readLengthPrefixed(u32) orelse return error.Truncated;
        const header_count = reader.readU32() orelse return error.Truncated;

        // Read headers into stack-allocated buffers
        var hdr_keys: [64][]const u8 = undefined;
        var hdr_vals: [64][]const u8 = undefined;
        const hdr_n = @min(header_count, 64);
        var h: u32 = 0;
        while (h < header_count) : (h += 1) {
            const hk = reader.readLengthPrefixed(u32) orelse return error.Truncated;
            const hv = reader.readLengthPrefixed(u32) orelse return error.Truncated;
            if (h < 64) {
                hdr_keys[h] = hk;
                hdr_vals[h] = hv;
            }
        }

        // Reconstruct StreamID from wire format fields
        const record_id = StreamID{ .timestamp_ms = @intCast(@as(u64, @bitCast(timestamp_ms))), .sequence = msg_sequence };
        var id_buf: [64]u8 = undefined;
        const id_str = record_id.format(&id_buf) catch "0-0";
        last_id = record_id;

        // The tier byte, as the op table names it.
        const tier_str = if (std.enums.fromInt(proto.StorageTier, tier)) |t| @tagName(t) else "unknown";

        if (json_output and !is_follow) {
            if (i > 0) ctx.print(",", .{});
            if (hdr_n > 0) {
                ctx.print("{{\"id\":{f},\"timestamp_ms\":{d},\"tier\":{f},\"data\":{f},\"headers\":{{", .{ output.jsonString(id_str), timestamp_ms, output.jsonString(tier_str), output.jsonString(payload) });
                var hi: u32 = 0;
                while (hi < hdr_n) : (hi += 1) {
                    if (hi > 0) ctx.print(",", .{});
                    ctx.print("{f}:{f}", .{ output.jsonString(hdr_keys[hi]), output.jsonString(hdr_vals[hi]) });
                }
                ctx.print("}}}}", .{});
            } else {
                ctx.print("{{\"id\":{f},\"timestamp_ms\":{d},\"tier\":{f},\"data\":{f}}}", .{ output.jsonString(id_str), timestamp_ms, output.jsonString(tier_str), output.jsonString(payload) });
            }
        } else {
            if (hdr_n > 0) {
                ctx.print("{s} [{s}]: {s}", .{ id_str, tier_str, payload });
                var hi: u32 = 0;
                while (hi < hdr_n) : (hi += 1) {
                    ctx.print(" {s}={s}", .{ hdr_keys[hi], hdr_vals[hi] });
                }
                ctx.print("\n", .{});
            } else {
                ctx.print("{s} [{s}]: {s}\n", .{ id_str, tier_str, payload });
            }
        }
    }

    if (json_output and !is_follow) {
        ctx.print("]\n", .{});
    }

    return .{ .count = count, .last_id = last_id };
}

fn runCreate(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander

    const partitions = ctx.getUint("partitions") orelse 1;
    const retention = ctx.getUint("retention");
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // Convert retention hours to seconds for retention_age, or null
    const retention_age: ?u64 = if (retention) |r| r * 3600 else null;
    var response = client_mod.stream.create(&client, namespace, stream, @intCast(partitions), ctx.getUint64("retention-count"), retention_age) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();

    try outcome.check(ctx, response);

    if (json_output) {
        ctx.print("{{\"name\":{f},\"partitions\":{d},\"status\":\"created\"}}\n", .{ output.jsonString(stream), partitions });
    } else {
        ctx.print("Created stream: {s}\n", .{stream});
    }
}

fn runAlter(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander

    const retention = ctx.getUint("retention");
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    // Convert retention hours to seconds for retention_age, or null
    const retention_age: ?u64 = if (retention) |r| r * 3600 else null;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var response = client_mod.stream.alter(&client, namespace, stream, ctx.getUint64("retention-count"), retention_age) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();

    try outcome.check(ctx, response);

    if (json_output) {
        ctx.print("{{\"name\":{f},\"status\":\"altered\"}}\n", .{output.jsonString(stream)});
    } else {
        ctx.print("Altered stream: {s}\n", .{stream});
    }
}

fn runDelete(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander

    const force = ctx.getBool("force");
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var response = client_mod.stream.delete(&client, namespace, stream, force) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();

    try outcome.check(ctx, response);

    if (json_output) {
        ctx.print("{{\"name\":{f},\"status\":\"deleted\"}}\n", .{output.jsonString(stream)});
    } else {
        ctx.print("Deleted stream: {s}\n", .{stream});
    }
}

fn runInfo(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // info(client, namespace, stream)
    var response = client_mod.stream.info(&client, namespace, stream) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();

    try outcome.check(ctx, response);

    // Parse wire format: [first_ts:u64][first_seq:u64][last_ts:u64][last_seq:u64][count:u64][bytes:u64][partition_count:u32][retention_age_s:u64][retention_count:u64][retention_bytes:u64]
    var reader = wire.WireReader.init(response.data);
    const what = "stream info";
    const first_ts = reader.readU64() orelse return outcome.malformed(ctx, what);
    const first_seq = reader.readU64() orelse return outcome.malformed(ctx, what);
    const last_ts = reader.readU64() orelse return outcome.malformed(ctx, what);
    const last_seq = reader.readU64() orelse return outcome.malformed(ctx, what);
    const msg_count = reader.readU64() orelse return outcome.malformed(ctx, what);
    const bytes = reader.readU64() orelse return outcome.malformed(ctx, what);
    const partitions = reader.readU32() orelse return outcome.malformed(ctx, what);
    const retention_age_s = reader.readU64() orelse return outcome.malformed(ctx, what);
    const retention_count = reader.readU64() orelse return outcome.malformed(ctx, what);
    const retention_bytes = reader.readU64() orelse return outcome.malformed(ctx, what);

    // Reconstruct full StreamIDs with timestamp + sequence
    const first_id = StreamID{ .timestamp_ms = first_ts, .sequence = first_seq };
    const last_id = StreamID{ .timestamp_ms = last_ts, .sequence = last_seq };
    var first_buf: [64]u8 = undefined;
    var last_buf: [64]u8 = undefined;
    const first_str = first_id.format(&first_buf) catch "0-0";
    const last_str = last_id.format(&last_buf) catch "0-0";

    if (json_output) {
        ctx.print("{{\"name\":{f},\"namespace\":{f},\"partitions\":{d},\"message_count\":{d},\"size_bytes\":{d}", .{ output.jsonString(stream), output.jsonString(namespace), partitions, msg_count, bytes });
        if (msg_count > 0) {
            ctx.print(",\"first_id\":{f},\"last_id\":{f}", .{ output.jsonString(first_str), output.jsonString(last_str) });
        }
        if (retention_age_s > 0 or retention_count > 0 or retention_bytes > 0) {
            ctx.print(",\"retention\":{{", .{});
            var need_comma = false;
            if (retention_age_s > 0) {
                ctx.print("\"age_s\":{d}", .{retention_age_s});
                need_comma = true;
            }
            if (retention_count > 0) {
                if (need_comma) ctx.print(",", .{});
                ctx.print("\"count\":{d}", .{retention_count});
                need_comma = true;
            }
            if (retention_bytes > 0) {
                if (need_comma) ctx.print(",", .{});
                ctx.print("\"bytes\":{d}", .{retention_bytes});
            }
            ctx.print("}}", .{});
        }
        ctx.print("}}\n", .{});
    } else {
        ctx.print("Stream: {s}\n", .{stream});
        ctx.print("  Namespace: {s}\n", .{namespace});
        ctx.print("  Partitions: {d}\n", .{partitions});
        ctx.print("  Records: {d}\n", .{msg_count});
        ctx.print("  Size: {d} bytes\n", .{bytes});
        if (msg_count > 0) {
            ctx.print("  First ID: {s}\n", .{first_str});
            ctx.print("  Last ID: {s}\n", .{last_str});
        }
        if (retention_age_s > 0 or retention_count > 0 or retention_bytes > 0) {
            ctx.print("  Retention:\n", .{});
            if (retention_age_s > 0) ctx.print("    Age: {d}s\n", .{retention_age_s});
            if (retention_count > 0) ctx.print("    Count: {d}\n", .{retention_count});
            if (retention_bytes > 0) ctx.print("    Bytes: {d}\n", .{retention_bytes});
        }
    }
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const limit = ctx.getUint("limit") orelse 100;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // Accumulate raw row bytes from paginated wire responses.
    // Server sends: [count:u32]([name_len:u32][name][partition_count:u32])*[has_more:u8][cursor_len:u16][cursor]?
    // We copy the raw row bytes (between count and trailer) and track names for dedup.
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
        var response = client_mod.stream.list(&client, namespace, @intCast(limit), cursor) catch |err| return outcome.requestFailed(ctx, err);
        defer response.deinit();

        try outcome.check(ctx, response);

        if (response.data.len == 0) break;

        var reader = wire.WireReader.init(response.data);
        const count = reader.readU32() orelse return outcome.malformed(ctx, "stream list");

        var i: u32 = 0;
        while (i < count) : (i += 1) {
            // Mark row start to capture raw bytes
            const row_start = reader.pos;

            const name_len = reader.readU32() orelse return outcome.malformed(ctx, "stream list");
            const name = reader.readSlice(name_len) orelse return outcome.malformed(ctx, "stream list");
            _ = reader.readU32() orelse return outcome.malformed(ctx, "stream list"); // partition_count
            _ = reader.readU64() orelse return outcome.malformed(ctx, "stream list"); // retention_age_s
            _ = reader.readU64() orelse return outcome.malformed(ctx, "stream list"); // retention_count

            const row_end = reader.pos;

            // Dedup by name
            if (seen.get(name) != null) continue;
            const name_copy = try ctx.allocator.dupe(u8, name);
            seen.put(ctx.allocator, name_copy, {}) catch |err| {
                ctx.allocator.free(name_copy);
                return err;
            };

            // Copy raw row bytes
            try row_bytes.appendSlice(ctx.allocator, response.data[row_start..row_end]);
            entry_count += 1;
            if (entry_count >= limit) break;
        }

        const has_more = (reader.readU8() orelse return outcome.malformed(ctx, "stream list")) != 0;
        const cursor_len = reader.readU16() orelse return outcome.malformed(ctx, "stream list");
        const next_cursor = if (cursor_len > 0) reader.readSlice(cursor_len) orelse return outcome.malformed(ctx, "stream list") else null;

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
            ctx.print("No streams found in namespace '{s}'\n", .{namespace});
        }
        return;
    }

    // Build final wire buffer: [count:u32][accumulated_raw_rows]
    const buf = try ctx.allocator.alloc(u8, 4 + row_bytes.items.len);
    defer ctx.allocator.free(buf);

    std.mem.writeInt(u32, buf[0..4], entry_count, .little);
    @memcpy(buf[4..][0..row_bytes.items.len], row_bytes.items);

    output.printWireList(ctx, buf, "No streams found", &.{
        .{ .field = "name", .header = "NAME", .field_type = .str_u32 },
        .{ .field = "partitions", .header = "PARTITIONS", .field_type = .uint_u32, .alignment = .right },
        // 0: kept forever, or however many.
        .{ .field = "retention_age_s", .header = "MAX AGE (S)", .field_type = .uint_u64, .alignment = .right },
        .{ .field = "retention_count", .header = "MAX RECORDS", .field_type = .uint_u64, .alignment = .right },
    }) catch return outcome.malformed(ctx, "list");
}

fn runTrim(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander

    const before_str = ctx.getString("before");
    const maxlen = ctx.getChangedUint64("maxlen");
    const maxage = ctx.getChangedUint64("maxage");
    const dry_run = ctx.getBool("dry-run");
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    // Parse --before StreamID if provided
    var min_id: ?StreamID = null;
    if (before_str) |bs| {
        if (bs.len > 0) {
            const before_id = StreamID.parse(bs) catch {
                return outcome.usage(ctx, "Invalid StreamID format '{s}'. Use '<timestamp>-<sequence>' (e.g., '1703350800000-0')", .{bs});
            };
            min_id = before_id;
        }
    }

    if (maxlen == null and min_id == null and maxage == null) {
        return outcome.usage(ctx, "Specify one of --before, --maxlen or --maxage", .{});
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var response = client_mod.stream.trim(&client, namespace, stream, maxlen, min_id, maxage, dry_run) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();

    try outcome.check(ctx, response);

    const t = client_mod.stream.parseTrimmed(response) orelse return outcome.malformed(ctx, "trim response");
    if (json_output) {
        ctx.print("{{\"status\":\"{s}\",\"trimmed\":{d},\"first_seq\":{d}}}\n", .{ if (dry_run) "dry_run" else "ok", t.removed, t.first_seq });
    } else if (dry_run) {
        ctx.print("Would trim {d} records (dry run)\n", .{t.removed});
    } else {
        ctx.print("Trimmed {d} records\n", .{t.removed});
    }
}

fn runGroupRead(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander
    const group = ctx.getString("group") orelse "";
    const consumer = ctx.getString("consumer") orelse "";

    if (group.len == 0) {
        ctx.printErr("Error: --group is required\n", .{});
        return error.MissingRequiredArg;
    }
    if (consumer.len == 0) {
        ctx.printErr("Error: --consumer is required\n", .{});
        return error.MissingRequiredArg;
    }

    const limit = ctx.getUint("limit") orelse 1;
    const block = ctx.getChangedUint("block");
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var response = client_mod.stream.groupRead(
        &client,
        namespace,
        stream,
        group,
        consumer,
        @intCast(limit),
        if (block) |b| @intCast(b) else null,
    ) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();

    try outcome.check(ctx, response);

    // Checked whole first, then printed, so a cut answer prints nothing.
    printGroupRecords(null, response.data, stream, json_output) catch return outcome.malformed(ctx, "group records");
    printGroupRecords(ctx, response.data, stream, json_output) catch return outcome.malformed(ctx, "group records");
}

/// A record's delivery, as a read prints it and an ack, nack or touch
/// takes it: `term.index.id.deliveries`, the id as `ms-seq`.
const Delivery = struct { term: u64, index: u64, id: StreamID, deliveries: u32 };

fn parseDelivery(text: []const u8) ?Delivery {
    var it = std.mem.splitScalar(u8, text, '.');
    const term = std.fmt.parseInt(u64, it.next() orelse return null, 10) catch return null;
    const index = std.fmt.parseInt(u64, it.next() orelse return null, 10) catch return null;
    const id = StreamID.parse(it.next() orelse return null) catch return null;
    const deliveries = std.fmt.parseInt(u32, it.next() orelse return null, 10) catch return null;
    if (it.next() != null) return null;
    return .{ .term = term, .index = index, .id = id, .deliveries = deliveries };
}

/// Prints a group read's records; with no `out`, only checks them. The
/// answer is `[term:u64][index:u64][expires_at_ms:i64][count:u32]`, then each
/// record as a stream read gives it, followed by its `[deliveries:u32]`.
fn printGroupRecords(out: ?*commander.Context, data: []const u8, stream: []const u8, json_output: bool) error{Truncated}!void {
    const ctx: struct {
        c: ?*commander.Context,
        fn print(self: @This(), comptime fmt: []const u8, args: anytype) void {
            if (self.c) |c| c.print(fmt, args);
        }
    } = .{ .c = out };
    var reader = wire.WireReader.init(data);
    const term = reader.readU64() orelse return error.Truncated;
    const index = reader.readU64() orelse return error.Truncated;
    const expires_at_ms = reader.readI64() orelse return error.Truncated;
    const msg_count = reader.readU32() orelse return error.Truncated;

    if (msg_count == 0) {
        if (json_output) {
            ctx.print("[]\n", .{});
        } else {
            ctx.print("(no messages)\n", .{});
        }
        return;
    }

    if (json_output) ctx.print("[", .{});

    var i: u32 = 0;
    while (i < msg_count) : (i += 1) {
        const msg_sequence = reader.readU64() orelse return error.Truncated;
        const timestamp_ms = reader.readI64() orelse return error.Truncated;
        _ = reader.readU8() orelse return error.Truncated; // tier
        _ = reader.readU32() orelse return error.Truncated; // partition
        const key_present = reader.readU8() orelse return error.Truncated;
        if (key_present != 0) _ = reader.readLengthPrefixed(u32) orelse return error.Truncated;
        const payload = reader.readLengthPrefixed(u32) orelse return error.Truncated;
        const header_count = reader.readU32() orelse return error.Truncated;
        var hdr_keys: [64][]const u8 = undefined;
        var hdr_vals: [64][]const u8 = undefined;
        const hdr_n = @min(header_count, 64);
        var h: u32 = 0;
        while (h < header_count) : (h += 1) {
            const hk = reader.readLengthPrefixed(u32) orelse return error.Truncated;
            const hv = reader.readLengthPrefixed(u32) orelse return error.Truncated;
            if (h < 64) {
                hdr_keys[h] = hk;
                hdr_vals[h] = hv;
            }
        }
        const deliveries = reader.readU32() orelse return error.Truncated;

        const record_id = StreamID{ .timestamp_ms = @intCast(@as(u64, @bitCast(timestamp_ms))), .sequence = msg_sequence };
        var id_buf: [64]u8 = undefined;
        const id_str = record_id.format(&id_buf) catch "0-0";
        var dbuf: [128]u8 = undefined;
        const delivery = std.fmt.bufPrint(&dbuf, "{d}.{d}.{s}.{d}", .{ term, index, id_str, deliveries }) catch "";

        if (json_output) {
            if (i > 0) ctx.print(",", .{});
            ctx.print("{{\"delivery\":{f},\"id\":{f},\"deliveries\":{d},\"expires_at_ms\":{d},\"stream\":{f},\"data\":{f}", .{ output.jsonString(delivery), output.jsonString(id_str), deliveries, expires_at_ms, output.jsonString(stream), output.jsonString(payload) });
            if (hdr_n > 0) {
                ctx.print(",\"headers\":{{", .{});
                var hi: u32 = 0;
                while (hi < hdr_n) : (hi += 1) {
                    if (hi > 0) ctx.print(",", .{});
                    ctx.print("{f}:{f}", .{ output.jsonString(hdr_keys[hi]), output.jsonString(hdr_vals[hi]) });
                }
                ctx.print("}}", .{});
            }
            ctx.print("}}", .{});
        } else {
            ctx.print("{s} {s}", .{ delivery, payload });
            var hi: u32 = 0;
            while (hi < hdr_n) : (hi += 1) ctx.print(" {s}={s}", .{ hdr_keys[hi], hdr_vals[hi] });
            ctx.print("\n", .{});
        }
    }
    if (json_output) ctx.print("]\n", .{});
}

/// Report on records by their deliveries: one request per delivery, its
/// answer said record by record.
fn runGroupReport(ctx: *commander.Context, kind: client_mod.stream.GroupReport) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander
    const group = ctx.getString("group") orelse "";
    const consumer = ctx.getString("consumer") orelse "";
    const ids_str = ctx.getString("ids") orelse "";
    if (group.len == 0) {
        ctx.printErr("Error: --group is required\n", .{});
        return error.MissingRequiredArg;
    }
    if (ids_str.len == 0) {
        ctx.printErr("Error: --ids is required\n", .{});
        return error.MissingRequiredArg;
    }
    var nack: client_mod.stream.NackOptions = .{};
    if (kind == .nack) {
        nack.delay_ms = ctx.getUint("delay") orelse 0;
        if (nack.delay_ms > 86_400_000) return outcome.usage(ctx, "--delay is at most 86400000 (a day), not {d}", .{nack.delay_ms});
        nack.dead = ctx.getBool("dlq");
        nack.error_text = ctx.getString("error") orelse "";
        nack.abandon = ctx.getBool("abandon");
    }
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    var deliveries: std.ArrayList(Delivery) = .empty;
    defer deliveries.deinit(ctx.allocator);
    var iter = std.mem.splitScalar(u8, ids_str, ',');
    while (iter.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " ");
        if (trimmed.len == 0) continue;
        const d = parseDelivery(trimmed) orelse
            return outcome.usage(ctx, "'{s}' isn't a delivery: give term.index.id.deliveries, as a read printed it (e.g. 3.42.1703350800000-0.1)", .{trimmed});
        try deliveries.append(ctx.allocator, d);
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();
    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var applied: u64 = 0;
    var refused: u64 = 0;
    // Said once every request is answered, with the exit that says so.
    var said: std.ArrayList(struct { ms: u64, seq: u64, why: queue_ops.Why }) = .empty;
    defer said.deinit(ctx.allocator);
    var done = std.ArrayList(bool).empty;
    defer done.deinit(ctx.allocator);
    try done.appendNTimes(ctx.allocator, false, deliveries.items.len);
    if (json_output) ctx.print("{{\"refused\":[", .{});
    for (deliveries.items, 0..) |first, i| {
        if (done.items[i]) continue;
        var items: std.ArrayList(client_mod.stream.GroupItem) = .empty;
        defer items.deinit(ctx.allocator);
        for (deliveries.items[i..], i..) |d, j| if (d.term == first.term and d.index == first.index) {
            try items.append(ctx.allocator, .{ .id = d.id, .deliveries = d.deliveries });
            done.items[j] = true;
        };
        var response = client_mod.stream.groupReport(&client, namespace, stream, group, consumer, kind, first.term, first.index, items.items, nack) catch |err| return outcome.requestFailed(ctx, err);
        defer response.deinit();
        try outcome.check(ctx, response);
        var r = wire.WireReader.init(response.data);
        if (kind == .touch) _ = r.readI64() orelse return outcome.malformed(ctx, "touch answer");
        applied += r.readU32() orelse return outcome.malformed(ctx, "report answer");
        const n = r.readU32() orelse return outcome.malformed(ctx, "report answer");
        for (0..n) |_| {
            const ms = r.readU64() orelse return outcome.malformed(ctx, "report answer");
            const seq = r.readU64() orelse return outcome.malformed(ctx, "report answer");
            const why: queue_ops.Why = std.enums.fromInt(queue_ops.Why, r.readU8() orelse return outcome.malformed(ctx, "report answer")) orelse return outcome.malformed(ctx, "report answer");
            if (json_output) {
                if (refused > 0) ctx.print(",", .{});
                ctx.print("{{\"id\":\"{d}-{d}\",\"why\":\"{s}\",\"reason\":{f}}}", .{ ms, seq, @tagName(why), output.jsonString(groupWhy(why)) });
            } else {
                try said.append(ctx.allocator, .{ .ms = ms, .seq = seq, .why = why });
            }
            refused += 1;
        }
    }
    const verb = switch (kind) {
        .ack => "Acknowledged",
        .nack => if (nack.abandon) "Handed back" else "Failed",
        .touch => "Kept",
    };
    if (json_output) {
        ctx.print("],\"applied\":{d}}}\n", .{applied});
    } else {
        ctx.print("{s} {d} record(s)\n", .{ verb, applied });
    }
    if (refused > 0) {
        for (said.items) |x| ctx.printErr("Error: {d}-{d}: {s}\n", .{ x.ms, x.seq, groupWhy(x.why) });
        return error.Refused;
    }
}

/// What became of a record a report didn't apply to, said for a group.
fn groupWhy(w: queue_ops.Why) []const u8 {
    return switch (w) {
        .gone => "gone: not out to this group: acked already, trimmed, or never read",
        .delivered_again => "stale: the record was delivered again since",
        .other_holder => "stale: another consumer holds this delivery",
        .expired => "stale: the lease ran out and the record moved on",
        .released => "stale: the record was nacked",
        .not_held => "stale: the record isn't held",
    };
}

fn runGroupAck(ctx: *commander.Context) commander.Error!void {
    return runGroupReport(ctx, .ack);
}

fn runGroupNack(ctx: *commander.Context) commander.Error!void {
    return runGroupReport(ctx, .nack);
}

fn runGroupTouch(ctx: *commander.Context) commander.Error!void {
    return runGroupReport(ctx, .touch);
}

fn runGroupInfo(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander
    const group = ctx.getString("group") orelse "";

    if (group.len == 0) {
        ctx.printErr("Error: --group is required\n", .{});
        return error.MissingRequiredArg;
    }

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var response = client_mod.stream.groupInfo(&client, namespace, stream, group) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();

    try outcome.check(ctx, response);

    // [pending:u64][holders:u32][created_at_ns:u64][ready:u64][leased:u64]
    // [dead:u64][trimmed_pending:u64]
    var reader = wire.WireReader.init(response.data);
    const pending = reader.readU64() orelse return outcome.malformed(ctx, "group info");
    const holders = reader.readU32() orelse return outcome.malformed(ctx, "group info");
    const created_at_ns = reader.readU64() orelse return outcome.malformed(ctx, "group info");
    const ready = reader.readU64() orelse return outcome.malformed(ctx, "group info");
    const leased = reader.readU64() orelse return outcome.malformed(ctx, "group info");
    const dead = reader.readU64() orelse return outcome.malformed(ctx, "group info");
    const trimmed_pending = reader.readU64() orelse return outcome.malformed(ctx, "group info");

    if (json_output) {
        // JSON keeps the raw epoch-ns for machine consumers.
        ctx.print(
            "{{\"stream\":{f},\"group\":{f},\"pending\":{d},\"members\":{d},\"created_at_ns\":{d},\"ready\":{d},\"leased\":{d},\"dead\":{d},\"trimmed_pending\":{d}}}\n",
            .{ output.jsonString(stream), output.jsonString(group), pending, holders, created_at_ns, ready, leased, dead, trimmed_pending },
        );
    } else {
        const created_ms: i64 = @intCast(created_at_ns / std.time.ns_per_ms);
        const ts_buf = output.formatTimestamp(created_ms);
        ctx.print("Consumer Group: {s}/{s}\n", .{ stream, group });
        ctx.print("  Members:    {d} holding records\n", .{holders});
        ctx.print("  Ready:      {d}\n", .{ready});
        ctx.print("  Leased:     {d}\n", .{leased});
        ctx.print("  Dead:       {d}\n", .{dead});
        if (trimmed_pending > 0) ctx.print("  Trimmed, not yet cleaned: {d} (counted above)\n", .{trimmed_pending});
        ctx.print("  Created:    {s}\n", .{ts_buf[0..20]});
    }
}

fn runGroupPending(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander
    const group = ctx.getString("group") orelse "";

    if (group.len == 0) {
        ctx.printErr("Error: --group is required\n", .{});
        return error.MissingRequiredArg;
    }

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // Optional consumer filter
    const consumer_filter = ctx.getString("consumer") orelse "";
    const consumer_arg: ?[]const u8 = if (consumer_filter.len > 0) consumer_filter else null;

    var response = client_mod.stream.groupPendingForConsumer(&client, namespace, stream, group, consumer_arg) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();

    try outcome.check(ctx, response);

    const data = response.data;

    // Parse pending wire format: [count:u32]([id:16][consumer_len:u32][consumer][delivered_at:i64][delivery_count:u32])*
    if (data.len < 4) {
        if (json_output) {
            ctx.print("{{\"stream\":{f},\"group\":{f},\"pending\":[],\"count\":0}}\n", .{ output.jsonString(stream), output.jsonString(group) });
        } else {
            ctx.print("Consumer Group: {s}/{s}\n  (no pending messages)\n", .{ stream, group });
        }
        return;
    }

    const count = std.mem.readInt(u32, data[0..4], .little);

    if (json_output) {
        ctx.print("{{\"stream\":{f},\"group\":{f},\"count\":{d},\"pending\":[", .{ output.jsonString(stream), output.jsonString(group), count });
    } else {
        ctx.print("Consumer Group: {s}/{s}\n  Pending: {d} message(s)\n\n", .{ stream, group, count });
        if (count > 0) {
            ctx.print("  {s:<24} {s:<16} {s}\n", .{ "ID", "CONSUMER", "DELIVERIES" });
            ctx.print("  {s:<24} {s:<16} {s}\n", .{ stdx.repeat("─", 24), stdx.repeat("─", 16), stdx.repeat("─", 10) });
        }
    }

    // Wire format (serializePendingEntries):
    // [count:u32]([ts:u64][seq:u64][delivery_count:u32][consumer_len:u16][consumer])*
    var offset: usize = 4;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        // StreamID: 16 bytes (timestamp_ms:u64 + sequence:u64)
        if (offset + 16 > data.len) break;
        const ts = std.mem.readInt(u64, data[offset..][0..8], .little);
        const seq = std.mem.readInt(u64, data[offset + 8 ..][0..8], .little);
        offset += 16;

        // delivery_count: u32
        if (offset + 4 > data.len) break;
        const delivery_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        // consumer_len: u16
        if (offset + 2 > data.len) break;
        const consumer_len = std.mem.readInt(u16, data[offset..][0..2], .little);
        offset += 2;

        // consumer: [consumer_len]u8
        if (offset + consumer_len > data.len) break;
        const consumer = data[offset..][0..consumer_len];
        offset += consumer_len;

        // Format ID string
        var id_buf: [64]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}-{d}", .{ ts, seq }) catch continue;

        if (json_output) {
            if (i > 0) ctx.print(",", .{});
            ctx.print("{{\"id\":{f},\"consumer\":{f},\"delivery_count\":{d}}}", .{ output.jsonString(id_str), output.jsonString(consumer), delivery_count });
        } else {
            ctx.print("  {s:<24} {s:<16} {d}\n", .{ id_str, consumer, delivery_count });
        }
    }

    if (json_output) {
        ctx.print("]}}\n", .{});
    }
}

fn runGroupCreate(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?;
    const group = ctx.getString("group") orelse "";

    if (group.len == 0) {
        ctx.printErr("Error: --group is required\n", .{});
        return error.MissingRequiredArg;
    }

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    const ack_timeout_raw = ctx.getUint("ack-timeout") orelse 30000;
    if (ack_timeout_raw < 5000 or ack_timeout_raw > 43_200_000) return outcome.usage(ctx, "--ack-timeout is 5000 to 43200000 ms, not {d}", .{ack_timeout_raw});
    const max_deliver_raw = ctx.getUint("max-deliver") orelse 5;
    if (max_deliver_raw < 1 or max_deliver_raw > 255) return outcome.usage(ctx, "--max-deliver is 1 to 255, not {d}", .{max_deliver_raw});
    const ack_timeout: u32 = @intCast(ack_timeout_raw);
    const max_deliver: u8 = @intCast(max_deliver_raw);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var response = client_mod.stream.groupCreate(&client, .{
        .namespace = namespace,
        .stream = stream,
        .group = group,
        .ack_timeout_ms = ack_timeout,
        .max_deliver = max_deliver,
    }) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();

    try outcome.check(ctx, response);
    // [created:u8]: 0 when the group was there with these settings.
    if (response.data.len != 1) return outcome.malformed(ctx, "group create answer");
    const created = response.data[0] != 0;

    if (json_output) {
        ctx.print("{{\"status\":\"ok\",\"created\":{},\"group\":{f},\"stream\":{f}}}\n", .{ created, output.jsonString(group), output.jsonString(stream) });
    } else if (created) {
        ctx.print("Created consumer group '{s}' on stream '{s}'\n", .{ group, stream });
    } else {
        ctx.print("Consumer group '{s}' on stream '{s}' exists, with these settings\n", .{ group, stream });
    }
}

fn runGroupDelete(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?;
    const group = ctx.getString("group") orelse "";

    if (group.len == 0) {
        ctx.printErr("Error: --group is required\n", .{});
        return error.MissingRequiredArg;
    }

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = output.getFormat(ctx) == .json;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var response = client_mod.stream.groupDelete(&client, namespace, stream, group) catch |err| return outcome.requestFailed(ctx, err);
    defer response.deinit();

    try outcome.check(ctx, response);

    if (json_output) {
        ctx.print("{{\"status\":\"ok\",\"group\":{f},\"stream\":{f},\"deleted\":true}}\n", .{ output.jsonString(group), output.jsonString(stream) });
    } else {
        ctx.print("Deleted consumer group '{s}' from stream '{s}'\n", .{ group, stream });
    }
}

// ==================== Testing ====================

test "create stream command" {
    const allocator = std.testing.allocator;

    const cmd = try createStreamCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("stream", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 5);
}

test "stream read: records cut short are Truncated, not fewer records" {
    var q = try outcome.QuietContext.init(std.testing.allocator);
    defer q.deinit();
    // [count:u32] then [seq:u64][ts:i64][tier:u8][partition:u32][key_present:u8][len:u32][payload][headers:u32]
    const record = [_]u8{1} ++ @as([7]u8, @splat(0)) ++ [_]u8{2} ++ @as([7]u8, @splat(0)) ++ [_]u8{0} ++ @as([4]u8, @splat(0)) ++ [_]u8{0} ++ [_]u8{ 1, 0, 0, 0 } ++ "x".* ++ @as([4]u8, @splat(0));
    // The second record carries one header, [klen u32][k][vlen u32][v], so a
    // cut inside a header's value is caught too.
    const with_header = [_]u8{1} ++ @as([7]u8, @splat(0)) ++ [_]u8{2} ++ @as([7]u8, @splat(0)) ++ [_]u8{0} ++ @as([4]u8, @splat(0)) ++ [_]u8{0} ++ [_]u8{ 1, 0, 0, 0 } ++ "x".* ++
        [_]u8{ 1, 0, 0, 0 } ++ [_]u8{ 1, 0, 0, 0 } ++ "k".* ++ [_]u8{ 3, 0, 0, 0 } ++ "val".*;
    const answer = [_]u8{ 2, 0, 0, 0 } ++ record ++ with_header;
    const data = try std.testing.allocator.dupe(u8, &answer);
    var whole: client_mod.Response = .{ .status = .ok, .data = data, .allocator = std.testing.allocator };
    defer whole.deinit();

    const got = try parseAndPrintRecords(&q.ctx, &whole, false, true, false);
    try std.testing.expectEqual(@as(u32, 2), got.count);

    var cut = whole;
    for (1..whole.data.len) |n| {
        cut.data = whole.data[0..n];
        try std.testing.expectError(error.Truncated, parseAndPrintRecords(null, &cut, false, true, false));
    }
    cut.data = whole.data[0..0];
    try std.testing.expectEqual(@as(u32, 0), (try parseAndPrintRecords(&q.ctx, &cut, false, true, false)).count);
}

test "stream read --follow ends with retryable (exit 4) on an unavailable answer, not a retry loop" {
    const allocator = std.testing.allocator;
    const server = try outcome.FakeServer.start(.unavailable, "no leader");
    defer server.stop();
    var ep: [32]u8 = undefined;
    const root = try outcome.testRoot(allocator, try createStreamCommand(allocator));
    defer root.deinit();
    const result = root.executeSlice(&.{ "flo", "stream", "read", "s", "--follow", "--endpoint", server.endpoint(&ep) });
    try std.testing.expectError(error.Retryable, result);
    try std.testing.expectEqual(@as(u8, 4), outcome.exitCode(result));
}

test "stream group read: records cut short are Truncated before anything prints" {
    // [term:u64][index:u64][expires_at_ms:i64][count:u32] then each
    // [seq:u64][ts:i64][tier:u8][partition:u32][key_present:u8][len:u32][payload]
    // [headers:u32]([klen:u32][k][vlen:u32][v])*[deliveries:u32]
    const record = [_]u8{1} ++ @as([7]u8, @splat(0)) ++ [_]u8{2} ++ @as([7]u8, @splat(0)) ++ [_]u8{0} ++ @as([4]u8, @splat(0)) ++ [_]u8{0} ++ [_]u8{ 1, 0, 0, 0 } ++ "x".* ++
        [_]u8{ 1, 0, 0, 0 } ++ [_]u8{ 1, 0, 0, 0 } ++ "k".* ++ [_]u8{ 1, 0, 0, 0 } ++ "v".* ++ [_]u8{ 1, 0, 0, 0 };
    const answer = @as([24]u8, @splat(0)) ++ [_]u8{ 2, 0, 0, 0 } ++ record ++ record;
    try printGroupRecords(null, &answer, "s", false);
    for (1..answer.len) |n| try std.testing.expectError(error.Truncated, printGroupRecords(null, answer[0..n], "s", false));
}
