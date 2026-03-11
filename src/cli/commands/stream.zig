//! Stream commands for Flo CLI using Commander framework
//!
//! StreamID Format: `<timestamp_ms>-<sequence>` (e.g., "1703350800000-0")
//!   - timestamp_ms: Unix milliseconds since epoch
//!   - sequence: Counter for multiple entries in same millisecond
//!   - Special values: "0" or "0-0" (beginning), "$" (latest/tail)
//!
//! Usage:
//!   flo stream create <stream> [--partitions N]
//!   flo stream append <stream> <payload>... [--header <key=value>]
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
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const wire = @import("../../util/wire.zig");
const StreamID = @import("../../stream/stream_id.zig").StreamID;
const output = @import("../output.zig");
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
        // Persistent flags - inherited by all subcommands
        .persistentFlag("namespace", .{ .short = 'n', .value = .{ .string = "default" }, .desc = "Namespace" })
        .persistentFlag("endpoint", .{ .short = 'e', .value = .{ .string = "" }, .desc = "Server endpoint (host:port)" })
        .subcommand(
            commander.newBuilder(allocator)
                .name("append")
                .about("Append records to a stream")
                .aliases(&.{"add"})
                .examples(&.{
                    "flo stream append events 'Hello, World!'",
                    "flo stream append logs '{\"level\":\"info\"}' --header type=json",
                    "flo stream append metrics value1 value2 value3",
                })
                .arg("stream", "Stream name")
                .variadicArg("payloads", "Record payload(s) - can specify multiple")
                .stringFlag("header", 'H', "", "Header key=value (repeatable)")
                .uintFlag("partition", 'p', 0, "Target partition")
                .stringFlag("partition-key", 'k', "", "Partition key for routing")
                .boolFlag("json", 'j', "Output in JSON format")
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
                    "flo stream read logs --json",
                })
                .arg("stream", "Stream name")
                .stringFlag("start", 's', "0-0", "Starting StreamID (timestamp-sequence, 0-0=beginning, $=latest)")
                .stringFlag("end", 'e', "", "Ending StreamID (timestamp-sequence, inclusive)")
                .uintFlag("limit", 'l', 10, "Maximum records to read")
                .boolFlag("follow", 'f', "Follow mode - continuously tail for new records (like tail -f)")
                .uintFlag("block", 'b', 0, "Block for new data (ms, 0=forever). Single read unlike --follow.")
                .uintFlag("partition", 'P', 0, "Partition to read from (default: 0)")
                .stringFlag("partition-key", 'k', "", "Partition key for routing (reads from same partition as append)")
                .boolFlag("json", 'j', "Output in JSON format")
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
                .uintFlag("partitions", 'p', 1, "Number of partitions")
                .uintFlag("retention", 'r', 0, "Retention period (hours, 0=forever)")
                .boolFlag("json", 'j', "Output in JSON format")
                .action(wrapHandler(runCreate)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("info")
                .about("Show stream information")
                .examples(&.{
                    "flo stream info events",
                    "flo stream info events --json",
                })
                .arg("stream", "Stream name")
                .boolFlag("json", 'j', "Output in JSON format")
                .action(wrapHandler(runInfo)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List all streams")
                .aliases(&.{"ls"})
                .uintFlag("limit", 'l', 100, "Maximum streams to list")
                .boolFlag("json", 'j', "Output in JSON format")
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
                .uint64Flag("maxbytes", 0, 0, "Trim to this size in bytes")
                .boolFlag("dry-run", 'd', "Show what would be trimmed without trimming")
                .boolFlag("json", 'j', "Output in JSON format")
                .action(wrapHandler(runTrim)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("group")
                .about("Consumer group operations")
                .aliases(&.{"cg"})
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("read")
                        .about("Read from consumer group")
                        .examples(&.{
                            "flo stream group read events --group mygroup --consumer worker1",
                            "flo stream group read events --group mygroup --consumer worker1 --limit 10",
                            "flo stream group read events --group mygroup --consumer worker1 --mode exclusive",
                            "flo stream group read events --group mygroup --consumer worker1 --mode key_shared --slots 16",
                            "flo stream group read events --group mygroup --consumer worker1 --no-ack",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("consumer", 'c', "", "Consumer ID (required)")
                        .uintFlag("limit", 'l', 1, "Maximum records to read")
                        .uintFlag("block", 'b', 0, "Block timeout (ms, 0=wait forever)")
                        .stringFlag("mode", 'm', "", "Consumer mode: shared, exclusive, key_shared")
                        .uintFlag("max-standbys", 0, 0, "Max standby consumers in exclusive mode (0=singleton, no standbys)")
                        .uintFlag("slots", 's', 16, "Number of hash slots for key_shared mode")
                        .uintFlag("ack-timeout", 0, 0, "Ack timeout (ms) before auto-redeliver")
                        .uintFlag("max-deliver", 0, 0, "Max delivery attempts before DLQ (0=unlimited)")
                        .uintFlag("redeliver-delay", 0, 0, "Delay before NACK'd message visible (ms)")
                        .boolFlag("no-ack", 0, "Auto-ack on delivery (at-most-once)")
                        .boolFlag("json", 'j', "Output in JSON format")
                        .action(wrapHandler(runGroupRead)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("ack")
                        .about("Acknowledge records by StreamID")
                        .examples(&.{
                            "flo stream group ack events --group mygroup --ids 1703350800000-0,1703350800000-1",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("consumer", 'c', "", "Consumer ID (for correct pending key matching)")
                        .stringFlag("ids", 'i', "", "Comma-separated StreamIDs to acknowledge")
                        .boolFlag("json", 'j', "Output in JSON format")
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
                        .boolFlag("json", 'j', "Output in JSON format")
                        .action(wrapHandler(runGroupInfo)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("join")
                        .about("Join a consumer group")
                        .arg("stream", "Stream name")
                        .arg("group", "Group name")
                        .arg("consumer", "Consumer ID")
                        .action(wrapHandler(runGroupJoin)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("pending")
                        .about("Show pending messages in a consumer group")
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .boolFlag("json", 'j', "Output in JSON format")
                        .action(wrapHandler(runGroupPending)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("nack")
                        .about("Release messages back for redelivery")
                        .examples(&.{
                            "flo stream group nack events --group mygroup --ids 1703350800000-0,1703350800000-1",
                            "flo stream group nack events --group mygroup --ids 1703350800000-0 --delay 5000",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("consumer", 'c', "", "Consumer ID (for correct pending key matching)")
                        .stringFlag("ids", 'i', "", "Comma-separated StreamIDs to release")
                        .uintFlag("delay", 0, 0, "Redelivery delay (ms) before message visible")
                        .boolFlag("json", 'j', "Output in JSON format")
                        .action(wrapHandler(runGroupNack)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("touch")
                        .about("Extend ack deadline for pending messages")
                        .examples(&.{
                            "flo stream group touch events --group mygroup --consumer worker1 --ids 1703350800000-0",
                            "flo stream group touch events --group mygroup --consumer worker1 --ids 1703350800000-0,1703350800000-1 --extend 60000",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("consumer", 'c', "", "Consumer ID that owns the messages (required)")
                        .stringFlag("ids", 'i', "", "Comma-separated StreamIDs to touch")
                        .uintFlag("extend", 0, 0, "Extra time (ms) to extend deadline (default: ack_timeout_ms)")
                        .boolFlag("json", 'j', "Output in JSON format")
                        .action(wrapHandler(runGroupTouch)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("leave")
                        .about("Remove a consumer from a group")
                        .examples(&.{
                            "flo stream group leave events --group mygroup --consumer worker1",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("consumer", 'c', "", "Consumer ID to remove")
                        .boolFlag("json", 'j', "Output in JSON format")
                        .action(wrapHandler(runGroupLeave)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("create")
                        .about("Create a consumer group with configuration")
                        .longAbout(
                            \\Create a consumer group with explicit settings.
                            \\
                            \\Settings are defined once at creation and apply to all consumers.
                            \\This is the recommended way to configure consumer groups.
                            \\
                            \\Modes:
                            \\  shared      Multiple consumers compete for messages (default)
                            \\  exclusive   Single active consumer, standbys auto-promoted on disconnect
                            \\  key_shared  Messages with same partition key go to same consumer
                        )
                        .examples(&.{
                            "flo stream group create events --group processors",
                            "flo stream group create events --group singleton --mode exclusive --max-standbys 0",
                            "flo stream group create events --group ordered --mode key_shared --slots 256",
                            "flo stream group create events --group reliable --ack-timeout 60000 --max-deliver 5",
                        })
                        .arg("stream", "Stream name")
                        .stringFlag("group", 'g', "", "Consumer group name (required)")
                        .stringFlag("mode", 'm', "shared", "Consumer mode: shared, exclusive, key_shared")
                        .uintFlag("slots", 's', 256, "Number of hash slots for key_shared mode")
                        .uintFlag("max-standbys", 0, 65535, "Max standby consumers (exclusive mode, 0=singleton, 65535=unlimited)")
                        .uintFlag("ack-timeout", 0, 30000, "Ack timeout (ms) before auto-redeliver")
                        .uintFlag("max-deliver", 0, 10, "Max delivery attempts before DLQ (0=unlimited)")
                        .uintFlag("redeliver-delay", 0, 0, "Delay before NACK'd message visible (ms)")
                        .boolFlag("no-ack", 0, "Auto-ack on delivery (at-most-once)")
                        .boolFlag("json", 'j', "Output in JSON format")
                        .action(wrapHandler(runGroupCreate)),
                )
                .subcommand(
                commander.newBuilder(allocator)
                    .name("delete")
                    .about("Delete a consumer group and all its state")
                    .longAbout(
                        \\Delete a consumer group, removing all associated state:
                        \\  - Group configuration
                        \\  - Committed offset
                        \\  - Active lease (exclusive mode)
                        \\  - Pending messages
                        \\  - Consumer slot assignments
                        \\  - Standby consumer list
                        \\
                        \\This is a destructive operation and cannot be undone.
                    )
                    .examples(&.{
                        "flo stream group delete events --group processors",
                        "flo stream group delete events --group mygroup --json",
                    })
                    .arg("stream", "Stream name")
                    .stringFlag("group", 'g', "", "Consumer group name (required)")
                    .boolFlag("json", 'j', "Output in JSON format")
                    .action(wrapHandler(runGroupDelete)),
            ),
        )
        .build();
}



fn runAppend(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander
    const payloads = ctx.getVariadicArgs("payloads") orelse &[_][]const u8{};

    if (payloads.len == 0) {
        ctx.printErr("Error: at least one payload is required\n", .{});
        return error.MissingRequiredArg;
    }

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = ctx.getBool("json");
    const partition_opt = ctx.getChangedUint("partition");
    const pk_raw = ctx.getString("partition-key");
    const partition_key: ?[]const u8 = if (pk_raw) |pk| (if (pk.len > 0) pk else null) else null;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    const result = client_mod.stream.appendEx(
        &client,
        namespace,
        stream,
        payloads,
        null,
        partition_key,
        if (partition_opt) |p| @intCast(p) else null,
    ) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // Construct StreamID from the append response
    const stream_id = result.first_id;
    var id_buf: [64]u8 = undefined;
    const id_str = stream_id.format(&id_buf) catch "0-0";

    if (json_output) {
        if (payloads.len == 1) {
            ctx.print("{{\"stream_id\":\"{s}\",\"count\":1}}\n", .{id_str});
        } else {
            const last_id = result.last_id;
            var last_buf: [64]u8 = undefined;
            const last_str = last_id.format(&last_buf) catch "0-0";
            ctx.print("{{\"first_id\":\"{s}\",\"last_id\":\"{s}\",\"count\":{d}}}\n", .{ id_str, last_str, payloads.len });
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
    const json_output = ctx.getBool("json");

    // Parse StreamID from start string
    var current_start = StreamID.parse(start_str) catch {
        ctx.printErr("Error: Invalid StreamID format '{s}'. Use '<timestamp>-<sequence>' (e.g., '1703350800000-0')\n", .{start_str});
        return error.CommandFailed;
    };

    // Parse optional end StreamID
    const end_id: ?StreamID = if (end_str) |es| blk: {
        if (es.len == 0) break :blk null;
        break :blk StreamID.parse(es) catch {
            ctx.printErr("Error: Invalid end StreamID format '{s}'. Use '<timestamp>-<sequence>' (e.g., '1703350800000-0')\n", .{es});
            return error.CommandFailed;
        };
    } else null;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    // Follow mode: continuous blocking reads with auto-advancing start position.
    // --follow implies --block 5000 (5 second polling interval).
    // --block N: single blocking read with N ms timeout.
    // Neither: single non-blocking read.
    const block_ms: ?u32 = if (follow) 5000 else if (block) |b| @intCast(b) else null;

    // For --follow mode, loop continuously; otherwise single read.
    var first_batch = true;
    while (true) {
        const start_mode: client_mod.stream.StartMode = if (current_start.eql(StreamID.MAX)) .tail else .stream_id;

        var response = client_mod.stream.read(&client, namespace, stream, start_mode, current_start, end_id, @intCast(limit), block_ms, if (read_partition) |rp| @intCast(rp) else null, partition_key) catch |err| {
            ctx.printErr("Request failed: {}\n", .{err});
            return error.CommandFailed;
        };
        defer response.deinit();

        if (response.isError()) {
            ctx.printErr("Error: {s}\n", .{response.errorMessage()});
            return error.CommandFailed;
        }

        const result = parseAndPrintRecords(ctx, &response, json_output, first_batch, follow);
        first_batch = false;

        if (result.last_id) |last_id| {
            // Advance start position past the last record we received
            current_start = last_id.next();
        }

        // If not in follow mode, we're done after one read
        if (!follow) break;
    }

    if (json_output and !follow) {
        // JSON array was already closed by parseAndPrintRecords for non-follow
    }
}

/// Result from parsing and printing stream records
const ParseResult = struct {
    count: u32,
    last_id: ?StreamID,
};

/// Parse response wire format and print records. Returns the last StreamID seen.
fn parseAndPrintRecords(
    ctx: *commander.Context,
    response: *client_mod.Response,
    json_output: bool,
    first_batch: bool,
    is_follow: bool,
) ParseResult {
    _ = first_batch;
    if (response.data.len < 4) {
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
    const count = reader.readU32() orelse {
        if (!is_follow) {
            if (json_output) {
                ctx.print("[]\n", .{});
            } else {
                ctx.print("No records\n", .{});
            }
        }
        return .{ .count = 0, .last_id = null };
    };

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
        const msg_sequence = reader.readU64() orelse break;
        const timestamp_ms = reader.readI64() orelse break;
        const tier = reader.readU8() orelse break;
        _ = reader.readU32() orelse break; // partition
        const key_present = reader.readU8() orelse break;
        if (key_present != 0) {
            // Skip key if present
            _ = reader.readLengthPrefixed(u32) orelse break;
        }
        const payload = reader.readLengthPrefixed(u32) orelse break;
        const header_count = reader.readU32() orelse break;
        // Skip headers
        var h: u32 = 0;
        while (h < header_count) : (h += 1) {
            _ = reader.readLengthPrefixed(u32) orelse break;
            _ = reader.readLengthPrefixed(u32) orelse break;
        }

        // Reconstruct StreamID from wire format fields
        const record_id = StreamID{ .timestamp_ms = @intCast(@as(u64, @bitCast(timestamp_ms))), .sequence = msg_sequence };
        var id_buf: [64]u8 = undefined;
        const id_str = record_id.format(&id_buf) catch "0-0";
        last_id = record_id;

        // Map tier byte to string: 0=hot, 1=warm, 2=cold
        const tier_str = switch (tier) {
            0 => "hot",
            1 => "warm",
            2 => "cold",
            else => "unknown",
        };

        if (json_output and !is_follow) {
            if (i > 0) ctx.print(",", .{});
            ctx.print("{{\"id\":\"{s}\",\"timestamp_ms\":{d},\"tier\":\"{s}\",\"data\":\"{s}\"}}", .{ id_str, timestamp_ms, tier_str, payload });
        } else {
            ctx.print("{s} [{s}]: {s}\n", .{ id_str, tier_str, payload });
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
    const json_output = ctx.getBool("json");

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // create(client, namespace, stream, partition_count, retention_count, retention_age, retention_bytes)
    // Convert retention hours to seconds for retention_age, or null
    const retention_age: ?u64 = if (retention) |r| r * 3600 else null;
    var response = client_mod.stream.create(&client, namespace, stream, @intCast(partitions), null, retention_age, null) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return error.CommandFailed;
    }

    if (json_output) {
        ctx.print("{{\"name\":\"{s}\",\"partitions\":{d},\"status\":\"created\"}}\n", .{ stream, partitions });
    } else {
        ctx.print("Created stream: {s}\n", .{stream});
    }
}

fn runInfo(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = ctx.getBool("json");

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // info(client, namespace, stream)
    var response = client_mod.stream.info(&client, namespace, stream) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return;
    }

    // Parse wire format: [first_ts:u64][first_seq:u64][last_ts:u64][last_seq:u64][count:u64][bytes:u64][partition_count:u32]
    var first_ts: u64 = 0;
    var first_seq: u64 = 0;
    var last_ts: u64 = 0;
    var last_seq: u64 = 0;
    var msg_count: u64 = 0;
    var bytes: u64 = 0;
    var partitions: u32 = 1;

    if (response.data.len >= 52) {
        var reader = wire.WireReader.init(response.data);
        first_ts = reader.readU64() orelse 0;
        first_seq = reader.readU64() orelse 0;
        last_ts = reader.readU64() orelse 0;
        last_seq = reader.readU64() orelse 0;
        msg_count = reader.readU64() orelse 0;
        bytes = reader.readU64() orelse 0;
        partitions = reader.readU32() orelse 1;
    }

    // Reconstruct full StreamIDs with timestamp + sequence
    const first_id = StreamID{ .timestamp_ms = first_ts, .sequence = first_seq };
    const last_id = StreamID{ .timestamp_ms = last_ts, .sequence = last_seq };
    var first_buf: [64]u8 = undefined;
    var last_buf: [64]u8 = undefined;
    const first_str = first_id.format(&first_buf) catch "0-0";
    const last_str = last_id.format(&last_buf) catch "0-0";

    if (json_output) {
        ctx.print("{{\"name\":\"{s}\",\"namespace\":\"{s}\",\"partitions\":{d},\"message_count\":{d},\"size_bytes\":{d}", .{ stream, namespace, partitions, msg_count, bytes });
        if (msg_count > 0) {
            ctx.print(",\"first_id\":\"{s}\",\"last_id\":\"{s}\"", .{ first_str, last_str });
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
    }
}

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

    // Collect all streams from all shards
    var all_streams: std.ArrayList(StreamEntry) = .empty;
    defer {
        for (all_streams.items) |entry| {
            ctx.allocator.free(entry.name);
        }
        all_streams.deinit(ctx.allocator);
    }

    var cursor: ?[]const u8 = null;
    var cursor_owned: ?[]u8 = null;
    defer if (cursor_owned) |c| ctx.allocator.free(c);

    // Walk all shards until no more data
    while (all_streams.items.len < limit) {
        var response = client_mod.stream.list(&client, namespace, @intCast(limit), cursor) catch |err| {
            ctx.printErr("Request failed: {}\n", .{err});
            return error.CommandFailed;
        };
        defer response.deinit();

        if (response.isError()) {
            ctx.printErr("Error: {s}\n", .{response.errorMessage()});
            return error.CommandFailed;
        }

        // Parse response data - wire format:
        // [count:u32] ([name_len:u32][name][partition_count:u32])* [has_more:u8] [cursor_len:u16][cursor]?
        if (response.data.len < 4) break;

        var reader = wire.WireReader.init(response.data);
        const count = reader.readU32() orelse break;

        // Read streams
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            const name_len = reader.readU32() orelse break;
            const name = reader.readSlice(name_len) orelse break;
            const partitions = reader.readU32() orelse break;

            // Check if already seen (dedup across shards)
            var found = false;
            for (all_streams.items) |entry| {
                if (std.mem.eql(u8, entry.name, name)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                const name_copy = ctx.allocator.dupe(u8, name) catch break;
                all_streams.append(ctx.allocator, .{ .name = name_copy, .partitions = partitions }) catch {
                    ctx.allocator.free(name_copy);
                    break;
                };
            }

            if (all_streams.items.len >= limit) break;
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
    if (all_streams.items.len == 0) {
        if (json_output) {
            ctx.print("[]\n", .{});
        } else {
            ctx.print("No streams found in namespace '{s}'\n", .{namespace});
        }
        return;
    }

    if (json_output) {
        ctx.print("[", .{});
        for (all_streams.items, 0..) |entry, idx| {
            if (idx > 0) ctx.print(",", .{});
            ctx.print("{{\"name\":\"{s}\",\"partitions\":{d}}}", .{ entry.name, entry.partitions });
        }
        ctx.print("]\n", .{});
    } else {
        ctx.print("Streams in namespace '{s}':\n", .{namespace});
        ctx.print("{s:<30} {s:<12}\n", .{ "NAME", "PARTITIONS" });
        ctx.print("{s}\n", .{"-" ** 42});
        for (all_streams.items) |entry| {
            ctx.print("{s:<30} {d:<12}\n", .{ entry.name, entry.partitions });
        }
    }
}

const StreamEntry = struct {
    name: []const u8,
    partitions: u32,
};

fn runTrim(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander

    const before_str = ctx.getString("before");
    const maxlen = ctx.getUint64("maxlen");
    const maxage = ctx.getUint64("maxage");
    const maxbytes = ctx.getUint64("maxbytes");
    const dry_run = ctx.getBool("dry-run");
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = ctx.getBool("json");

    // Parse --before StreamID if provided
    var min_id: ?StreamID = null;
    if (before_str) |bs| {
        if (bs.len > 0) {
            const before_id = StreamID.parse(bs) catch {
                ctx.printErr("Error: Invalid StreamID format '{s}'. Use '<timestamp>-<sequence>' (e.g., '1703350800000-0')\n", .{bs});
                return error.CommandFailed;
            };
            min_id = before_id;
        }
    }

    if (maxlen == null and min_id == null and maxage == null and maxbytes == null) {
        ctx.printErr("Error: Specify at least one of --before, --maxlen, --maxage, or --maxbytes\n", .{});
        return;
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    // trim(client, namespace, stream, max_len, min_id, max_age_seconds, max_bytes, dry_run)
    var response = client_mod.stream.trim(&client, namespace, stream, maxlen, min_id, maxage, maxbytes, dry_run) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return;
    }

    if (json_output) {
        if (dry_run) {
            ctx.print("{{\"status\":\"dry_run\",\"message\":\"would trim\"}}\n", .{});
        } else {
            // Try to parse trimmed_to from response
            if (before_str) |bs| {
                ctx.print("{{\"status\":\"ok\",\"trimmed_to\":\"{s}\"}}\n", .{bs});
            } else {
                ctx.print("{{\"status\":\"ok\"}}\n", .{});
            }
        }
    } else {
        if (dry_run) {
            ctx.print("(dry run) OK\n", .{});
        } else {
            if (before_str) |bs| {
                ctx.print("Trimmed to {s}\n", .{bs});
            } else {
                ctx.print("OK\n", .{});
            }
        }
    }
}

fn runGroupJoin(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?; // validated by commander
    const group = ctx.getPositional("group").?; // validated by commander
    const consumer = ctx.getPositional("consumer").?; // validated by commander

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // groupJoin(client, namespace, stream, group, consumer)
    var response = client_mod.stream.groupJoin(&client, namespace, stream, group, consumer) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("Joined group '{s}' as consumer '{s}'\n", .{ group, consumer });
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
    const json_output = ctx.getBool("json");

    // Parse consumer group options
    const mode_str = ctx.getString("mode") orelse "";
    const ack_timeout = ctx.getChangedUint("ack-timeout");
    const max_deliver = ctx.getChangedUint("max-deliver");
    const redeliver_delay = ctx.getChangedUint("redeliver-delay");
    const no_ack = ctx.getBool("no-ack");

    // Parse new options
    const max_standbys = ctx.getChangedUint("max-standbys");
    const slots = ctx.getChangedUint("slots");

    // Convert mode string to u8
    const mode: ?u8 = if (std.mem.eql(u8, mode_str, "exclusive"))
        1
    else if (std.mem.eql(u8, mode_str, "key_shared"))
        2
    else if (std.mem.eql(u8, mode_str, "shared") or mode_str.len == 0)
        null
    else {
        ctx.printErr("Error: --mode must be 'shared', 'exclusive', or 'key_shared'\n", .{});
        return error.MissingRequiredArg;
    };

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // groupReadWithOptions(client, namespace, stream, group, consumer, limit, block_ms, opts)
    var response = client_mod.stream.groupReadWithOptions(
        &client,
        namespace,
        stream,
        group,
        consumer,
        @intCast(limit),
        if (block) |b| @intCast(b) else null,
        .{
            .mode = mode,
            .max_standbys = if (max_standbys) |m| @intCast(m) else null,
            .num_slots = if (slots) |s| @intCast(s) else null,
            .ack_timeout_ms = if (ack_timeout) |t| @intCast(t) else null,
            .max_deliver = if (max_deliver) |m| @intCast(m) else null,
            .redelivery_delay_ms = if (redeliver_delay) |d| @intCast(d) else null,
            .no_ack = no_ack,
        },
    ) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return error.CommandFailed;
    }

    // Parse response data - wire format similar to stream read
    if (response.data.len < 4) {
        if (json_output) {
            ctx.print("[]\n", .{});
        } else {
            ctx.print("(no messages)\n", .{});
        }
        return;
    }

    var reader = wire.WireReader.init(response.data);
    const msg_count = reader.readU32() orelse {
        if (json_output) {
            ctx.print("[]\n", .{});
        } else {
            ctx.print("(no messages)\n", .{});
        }
        return;
    };

    if (msg_count == 0) {
        if (json_output) {
            ctx.print("[]\n", .{});
        } else {
            ctx.print("(no messages)\n", .{});
        }
        return;
    }

    if (json_output) {
        ctx.print("[", .{});
    }

    var i: u32 = 0;
    while (i < msg_count) : (i += 1) {
        const msg_sequence = reader.readU64() orelse break;
        const timestamp_ms = reader.readI64() orelse break;
        _ = reader.readU8() orelse break; // tier
        _ = reader.readU32() orelse break; // partition
        const key_present = reader.readU8() orelse break;
        if (key_present != 0) {
            _ = reader.readLengthPrefixed(u32) orelse break;
        }
        const payload = reader.readLengthPrefixed(u32) orelse break;
        const header_count = reader.readU32() orelse break;
        var h: u32 = 0;
        while (h < header_count) : (h += 1) {
            _ = reader.readLengthPrefixed(u32) orelse break;
            _ = reader.readLengthPrefixed(u32) orelse break;
        }

        const record_id = StreamID{ .timestamp_ms = @intCast(@as(u64, @bitCast(timestamp_ms))), .sequence = msg_sequence };
        var id_buf: [64]u8 = undefined;
        const id_str = record_id.format(&id_buf) catch "0-0";

        if (json_output) {
            if (i > 0) ctx.print(",", .{});
            ctx.print("{{\"id\":\"{s}\",\"timestamp_ms\":{d},\"data\":\"{s}\"}}", .{ id_str, timestamp_ms, payload });
        } else {
            ctx.print("{s}: {s}\n", .{ id_str, payload });
        }
    }

    if (json_output) {
        ctx.print("]\n", .{});
    }
}

fn runGroupAck(ctx: *commander.Context) commander.Error!void {
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

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = ctx.getBool("json");

    // Parse comma-separated StreamIDs
    var ids: std.ArrayList(StreamID) = .empty;
    defer ids.deinit(ctx.allocator);

    var iter = std.mem.splitScalar(u8, ids_str, ',');
    while (iter.next()) |id_str| {
        const trimmed = std.mem.trim(u8, id_str, " ");
        if (trimmed.len == 0) continue;

        const stream_id = StreamID.parse(trimmed) catch {
            ctx.printErr("Error: Invalid StreamID format '{s}'. Use '<timestamp>-<sequence>' (e.g., '1703350800000-0')\n", .{trimmed});
            return error.CommandFailed;
        };
        ids.append(ctx.allocator, stream_id) catch {
            ctx.printErr("Error: Out of memory\n", .{});
            return error.CommandFailed;
        };
    }

    if (ids.items.len == 0) {
        ctx.printErr("Error: No valid StreamIDs provided\n", .{});
        return error.MissingRequiredArg;
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // groupAck(client, namespace, stream, group, consumer, ids)
    var response = client_mod.stream.groupAck(&client, namespace, stream, group, consumer, ids.items) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return;
    }

    if (json_output) {
        ctx.print("{{\"status\":\"ok\",\"acknowledged\":{d}}}\n", .{ids.items.len});
    } else {
        ctx.print("Acknowledged {d} message(s)\n", .{ids.items.len});
    }
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
    const json_output = ctx.getBool("json");

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // groupPending(client, namespace, stream, group) - use pending for info
    var response = client_mod.stream.groupPending(&client, namespace, stream, group) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return;
    }

    if (json_output) {
        ctx.print("{{\"stream\":\"{s}\",\"group\":\"{s}\",\"pending_count\":{d}}}\n", .{ stream, group, response.data.len });
    } else {
        ctx.print("Consumer Group: {s}/{s}\n", .{ stream, group });
        ctx.print("  Pending: {d} bytes\n", .{response.data.len});
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
    const json_output = ctx.getBool("json");

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return;
    };

    // groupPending(client, namespace, stream, group)
    var response = client_mod.stream.groupPending(&client, namespace, stream, group) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return;
    }

    const data = response.data;

    // Parse pending wire format: [count:u32]([id:16][consumer_len:u32][consumer][delivered_at:i64][delivery_count:u32])*
    if (data.len < 4) {
        if (json_output) {
            ctx.print("{{\"stream\":\"{s}\",\"group\":\"{s}\",\"pending\":[],\"count\":0}}\n", .{ stream, group });
        } else {
            ctx.print("Consumer Group: {s}/{s}\n  (no pending messages)\n", .{ stream, group });
        }
        return;
    }

    const count = std.mem.readInt(u32, data[0..4], .little);

    if (json_output) {
        ctx.print("{{\"stream\":\"{s}\",\"group\":\"{s}\",\"count\":{d},\"pending\":[", .{ stream, group, count });
    } else {
        ctx.print("Consumer Group: {s}/{s}\n  Pending: {d} message(s)\n\n", .{ stream, group, count });
        if (count > 0) {
            ctx.print("  {s:<24} {s:<16} {s:<24} {s}\n", .{ "ID", "CONSUMER", "DELIVERED AT", "DELIVERIES" });
            ctx.print("  {s:<24} {s:<16} {s:<24} {s}\n", .{ "─" ** 24, "─" ** 16, "─" ** 24, "─" ** 10 });
        }
    }

    var offset: usize = 4;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        // StreamID: 16 bytes (timestamp_ms:u64 + sequence:u64)
        if (offset + 16 > data.len) break;
        const ts = std.mem.readInt(u64, data[offset..][0..8], .little);
        const seq = std.mem.readInt(u64, data[offset + 8 ..][0..8], .little);
        offset += 16;

        // consumer_len: u32
        if (offset + 4 > data.len) break;
        const consumer_len = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        // consumer: [consumer_len]u8
        if (offset + consumer_len > data.len) break;
        const consumer = data[offset..][0..consumer_len];
        offset += consumer_len;

        // delivered_at: i64
        if (offset + 8 > data.len) break;
        const delivered_at = std.mem.readInt(i64, data[offset..][0..8], .little);
        offset += 8;

        // delivery_count: u32
        if (offset + 4 > data.len) break;
        const delivery_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        // Format ID string
        var id_buf: [64]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}-{d}", .{ ts, seq }) catch continue;

        if (json_output) {
            if (i > 0) ctx.print(",", .{});
            ctx.print("{{\"id\":\"{s}\",\"consumer\":\"{s}\",\"delivered_at\":{d},\"delivery_count\":{d}}}", .{ id_str, consumer, delivered_at, delivery_count });
        } else {
            ctx.print("  {s:<24} {s:<16} {d:<24} {d}\n", .{ id_str, consumer, delivered_at, delivery_count });
        }
    }

    if (json_output) {
        ctx.print("]}}\n", .{});
    }
}

fn runGroupNack(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?;
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

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = ctx.getBool("json");
    const delay = ctx.getChangedUint("delay");

    // Parse StreamIDs for the wire protocol
    var ids: std.ArrayList(StreamID) = .empty;
    defer ids.deinit(ctx.allocator);

    var it = std.mem.splitScalar(u8, ids_str, ',');
    while (it.next()) |id_str| {
        const trimmed = std.mem.trim(u8, id_str, " ");
        if (trimmed.len == 0) continue;

        const stream_id = StreamID.parse(trimmed) catch {
            ctx.printErr("Error: Invalid StreamID format '{s}'. Use '<timestamp>-<sequence>' (e.g., '1703350800000-0')\n", .{trimmed});
            return error.CommandFailed;
        };
        ids.append(ctx.allocator, stream_id) catch {
            ctx.printErr("Error: Out of memory\n", .{});
            return error.CommandFailed;
        };
    }

    if (ids.items.len == 0) {
        ctx.printErr("Error: No valid StreamIDs provided\n", .{});
        return error.MissingRequiredArg;
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // Use nackWithDelay to support optional redelivery delay
    var response = client_mod.stream.groupNackWithDelay(
        &client,
        namespace,
        stream,
        group,
        consumer,
        ids.items,
        if (delay) |d| @intCast(d) else null,
    ) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return;
    }

    if (json_output) {
        ctx.print("{{\"status\":\"ok\",\"released\":{d}}}\n", .{ids.items.len});
    } else {
        ctx.print("Released {d} messages for redelivery\n", .{ids.items.len});
    }
}

fn runGroupTouch(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?;
    const group = ctx.getString("group") orelse "";
    const consumer = ctx.getString("consumer") orelse "";
    const ids_str = ctx.getString("ids") orelse "";

    if (group.len == 0) {
        ctx.printErr("Error: --group is required\n", .{});
        return error.MissingRequiredArg;
    }

    if (consumer.len == 0) {
        ctx.printErr("Error: --consumer is required\n", .{});
        return error.MissingRequiredArg;
    }

    if (ids_str.len == 0) {
        ctx.printErr("Error: --ids is required\n", .{});
        return error.MissingRequiredArg;
    }

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = ctx.getBool("json");
    const extend_ms = ctx.getChangedUint("extend");

    // Parse StreamIDs for the wire protocol
    var ids: std.ArrayList(StreamID) = .empty;
    defer ids.deinit(ctx.allocator);

    var it = std.mem.splitScalar(u8, ids_str, ',');
    while (it.next()) |id_str| {
        const trimmed = std.mem.trim(u8, id_str, " ");
        if (trimmed.len == 0) continue;

        const stream_id = StreamID.parse(trimmed) catch {
            ctx.printErr("Error: Invalid StreamID format '{s}'. Use '<timestamp>-<sequence>' (e.g., '1703350800000-0')\n", .{trimmed});
            return error.CommandFailed;
        };
        ids.append(ctx.allocator, stream_id) catch {
            ctx.printErr("Error: Out of memory\n", .{});
            return error.CommandFailed;
        };
    }

    if (ids.items.len == 0) {
        ctx.printErr("Error: No valid StreamIDs provided\n", .{});
        return error.MissingRequiredArg;
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    const result = client_mod.stream.groupTouch(
        &client,
        namespace,
        stream,
        group,
        consumer,
        ids.items,
        if (extend_ms) |ms| @intCast(ms) else null,
    ) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };

    if (json_output) {
        ctx.print("{{\"status\":\"ok\",\"touched\":{d}}}\n", .{result.touched_count});
    } else {
        ctx.print("Extended deadline for {d} message(s)\n", .{result.touched_count});
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
    const json_output = ctx.getBool("json");

    // Parse mode string to enum value
    const mode_str = ctx.getString("mode") orelse "shared";
    const mode: u8 = if (std.mem.eql(u8, mode_str, "exclusive"))
        1
    else if (std.mem.eql(u8, mode_str, "key_shared"))
        2
    else
        0; // shared

    const slots = @as(u16, @intCast(ctx.getUint("slots") orelse 256));
    const max_standbys_raw = ctx.getUint("max-standbys") orelse 65535;
    const max_standbys: ?u16 = if (max_standbys_raw == 65535) null else @as(u16, @intCast(max_standbys_raw));
    const ack_timeout = @as(u32, @intCast(ctx.getUint("ack-timeout") orelse 30000));
    const max_deliver = @as(u8, @intCast(ctx.getUint("max-deliver") orelse 10));
    const redeliver_delay = @as(u32, @intCast(ctx.getUint("redeliver-delay") orelse 0));
    const no_ack = ctx.getBool("no-ack");

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var response = client_mod.stream.groupCreate(&client, .{
        .namespace = namespace,
        .stream = stream,
        .group = group,
        .mode = mode,
        .num_slots = slots,
        .max_standbys = max_standbys,
        .ack_timeout_ms = ack_timeout,
        .max_deliver = max_deliver,
        .redelivery_delay_ms = redeliver_delay,
        .no_ack = no_ack,
    }) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return error.CommandFailed;
    }

    if (json_output) {
        ctx.print("{{\"status\":\"ok\",\"group\":\"{s}\",\"stream\":\"{s}\",\"mode\":\"{s}\"}}\n", .{ group, stream, mode_str });
    } else {
        ctx.print("Created consumer group '{s}' on stream '{s}' (mode: {s})\n", .{ group, stream, mode_str });
    }
}

fn runGroupLeave(ctx: *commander.Context) commander.Error!void {
    const stream = ctx.getPositional("stream").?;
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

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const json_output = ctx.getBool("json");

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var response = client_mod.stream.groupLeave(&client, namespace, stream, group, consumer) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return;
    }

    if (json_output) {
        ctx.print("{{\"status\":\"ok\",\"consumer\":\"{s}\",\"group\":\"{s}\"}}\n", .{ consumer, group });
    } else {
        ctx.print("Consumer '{s}' left group '{s}'\n", .{ consumer, group });
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
    const json_output = ctx.getBool("json");

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var response = client_mod.stream.groupDelete(&client, namespace, stream, group) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer response.deinit();

    if (response.isError()) {
        ctx.printErr("Error: {s}\n", .{response.errorMessage()});
        return error.CommandFailed;
    }

    if (json_output) {
        ctx.print("{{\"status\":\"ok\",\"group\":\"{s}\",\"stream\":\"{s}\",\"deleted\":true}}\n", .{ group, stream });
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
