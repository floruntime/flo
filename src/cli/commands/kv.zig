//! KV (Key-Value) commands for Flo CLI.
//!
//! Every command but `mget` is generated from the KV op table (cli/gen.zig):
//! its flags are the op's fields, and its answer prints by the op's response
//! schema. `mget` sends as many requests as its keys need, so it is written
//! here, on the same op.

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const outcome = @import("../outcome.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const output = @import("../output.zig");
const cli_config = @import("../config.zig");
const gen = @import("../gen.zig");
const kv_ops = @import("../../protocol/kv_ops.zig");
const schema = @import("../../protocol/schema.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the kv command tree
pub fn createKvCommand(allocator: Allocator) !*commander.Command {
    const kv = commander.newBuilder(allocator)
        .name("kv")
        .about("Key-Value store operations")
        .group("Data Commands")
        .longAbout(
        \\Interact with Flo's key-value store.
        \\
        \\Get, set and delete keys, with TTLs, conditions, transactions and
        \\JSON documents.
    );
    gen.addCommands(allocator, kv, &kv_ops.table, &kv_ops.groups);
    const mget_op = kv_ops.op("kv.mget");
    return kv.subcommand(
        commander.newBuilder(allocator)
            .name("mget")
            .about(mget_op.doc)
            .examples(&.{
                "flo kv mget key1 key2 key3",
                "flo kv mget user:1 user:2 user:3 -o json",
                "flo kv mget user:42:name user:42:visits -r user:42",
            })
            .variadicArg("keys", mget_op.request.field_list[0].doc)
            .stringFlag("routing-key", 'r', "", mget_op.request.field_list[1].doc)
            .action(wrapHandler(runMget)),
    ).build();
}

/// An ok answer to KV op `name`, decoded; its slices point into `resp`.
fn Answer(comptime name: []const u8) type {
    return struct {
        resp: client_mod.Response,
        value: kv_ops.op(name).response.Value,

        fn deinit(self: *@This()) void {
            self.resp.deinit();
        }
    };
}

/// Sends KV op `name` and decodes its answer; a refusal is reported and
/// becomes the command's outcome.
fn send(ctx: *commander.Context, client: *Client, comptime name: []const u8, namespace: []const u8, key: []const u8, body: kv_ops.op(name).request.Value) commander.Error!Answer(name) {
    var resp = client.call(kv_ops.op(name), namespace, key, body) catch |err| return outcome.requestFailed(ctx, err);
    errdefer resp.deinit();
    try outcome.check(ctx, resp);
    var diag: schema.Diagnostic = .{};
    const value = kv_ops.op(name).response.decodeAll(resp.data, &diag) catch return outcome.malformed(ctx, name ++ " answer");
    return .{ .resp = resp, .value = value };
}

fn dialClient(ctx: *commander.Context) commander.Error!Client {
    const endpoint = cli_config.getEndpoint(ctx);
    var client = Client.init(ctx.allocator, endpoint);
    client.connect() catch |err| {
        defer client.deinit();
        return outcome.connectFailed(ctx, err, endpoint);
    };
    return client;
}

fn optionalRoutingKey(ctx: *commander.Context) ?[]const u8 {
    const rk = ctx.getString("routing-key") orelse return null;
    return if (rk.len > 0) rk else null;
}

fn runMget(ctx: *commander.Context) commander.Error!void {
    const keys = ctx.getVariadicArgs("keys") orelse &.{};
    if (keys.len == 0) return outcome.usage(ctx, "at least one key is required", .{});
    if (keys.len > kv_ops.MAX_MGET) return outcome.usage(ctx, "at most {d} keys per request", .{kv_ops.MAX_MGET});
    const namespace = cli_config.getNamespace(ctx);
    const format = output.getFormat(ctx);
    var client = try dialClient(ctx);
    defer client.deinit();

    const Keys = kv_ops.op("kv.mget").request.field_list[0].T;
    const buf = try ctx.allocator.alloc(u8, Keys.max_size);
    defer ctx.allocator.free(buf);
    if (format == .json) ctx.print("[", .{});
    if (format == .table) ctx.print("{s:<30} {s:<40} {s:>10}\n{s:-<30} {s:-<40} {s:-<10}\n", .{ "KEY", "VALUE", "VERSION", "", "", "" });
    // The server answers as many as fit one answer, and says where to go on.
    var from: usize = 0;
    while (from < keys.len) {
        var list = Keys.Builder.init(buf);
        for (keys[from..]) |k| if (!(list.add(k) catch return outcome.usage(ctx, "a key is longer than {d} bytes", .{kv_ops.op("kv.mget").request.field_list[0].T.Element.bound}))) break;
        var a = try send(ctx, &client, "kv.mget", namespace, "", .{ .keys = list.value(), .routing_key = optionalRoutingKey(ctx) });
        defer a.deinit();
        var it = a.value.entries.iterator();
        var i = from;
        while (it.next() catch return outcome.malformed(ctx, "kv.mget answer")) |e| : (i += 1) {
            // More entries than keys sent would name keys never asked for.
            if (i - from == list.count) return outcome.malformed(ctx, "kv.mget answer");
            const value: ?[]const u8 = if (e.found) e.value else null;
            switch (format) {
                .json => {
                    if (i > 0) ctx.print(",", .{});
                    const Entry = struct { key: output.JsonString, value: ?output.JsonString, version: ?u64 };
                    output.Json.printInline(ctx, ctx.allocator, Entry{ .key = output.jsonString(keys[i]), .value = if (value) |v| output.jsonString(v) else null, .version = if (e.found) e.version else null });
                },
                .table => {
                    var ver_buf: [32]u8 = undefined;
                    const ver = if (e.found) std.fmt.bufPrint(&ver_buf, "{d}", .{e.version}) catch "" else "";
                    ctx.print("{s:<30} {s:<40} {s:>10}\n", .{ keys[i], value orelse "(nil)", ver });
                },
                .raw => ctx.print("{s}\t{s}\n", .{ keys[i], value orelse "(nil)" }),
            }
        }
        // A request holds as many keys as fit it; the answer, as many of
        // those as fit an answer. Go on from the first key either left out.
        const answered = a.value.next orelse list.count;
        if (answered == 0 or answered > list.count or i - from != answered) return outcome.malformed(ctx, "kv.mget answer");
        from += answered;
    }
    if (format == .json) ctx.print("]\n", .{});
}

// ==================== Testing ====================

test "kv: every generated command and group is in the tree" {
    const cmd = try createKvCommand(std.testing.allocator);
    defer cmd.deinit();
    try std.testing.expectEqualStrings("kv", cmd.name);
    var names: [32][]const u8 = undefined;
    var n: usize = 0;
    for (cmd.commands.items) |c| {
        names[n] = c.name;
        n += 1;
    }
    for ([_][]const u8{ "get", "set", "delete", "scan", "history", "incr", "expire", "exists", "mget", "json", "txn" }) |want| {
        for (names[0..n]) |have| {
            if (std.mem.eql(u8, have, want)) break;
        } else return error.TestUnexpectedResult;
    }
    for (cmd.commands.items) |c| if (std.mem.eql(u8, c.name, "txn")) {
        try std.testing.expectEqual(@as(usize, 3), c.commands.items.len);
    };
}

fn run(args: []const []const u8) commander.Error!void {
    const root = try outcome.testRoot(std.testing.allocator, try createKvCommand(std.testing.allocator));
    defer root.deinit();
    return root.executeSlice(args);
}

test "kv: each kind of flag is refused by name before anything is sent" {
    // No server: a refusal here can't have come from one.
    const big = try std.testing.allocator.alloc(u8, kv_ops.MAX_VALUE + 1);
    defer std.testing.allocator.free(big);
    @memset(big, 'v');
    for ([_][]const []const u8{
        &.{ "flo", "kv", "set", "k", "v", "--ttl-ms", "5q" }, // a duration
        &.{ "flo", "kv", "get", "k", "--wait-ms", "300001" }, // past its range
        &.{ "flo", "kv", "set", "k", "v", "--cas-version", "-1" }, // an unsigned integer
        &.{ "flo", "kv", "set", "k", "v", "--condition", "nope" }, // an enum
        &.{ "flo", "kv", "expire", "k" }, // a required field
        &.{ "flo", "kv", "scan", "--cursor", "zz" }, // hex
        &.{ "flo", "kv", "set", "k", big }, // bytes past their bound
        &.{ "flo", "kv", "txn", "commit", "x" }, // a positional integer
    }) |args| {
        try std.testing.expectError(error.Usage, run(args));
    }
}

/// Runs `args` against a server answering `body`, returning what it printed
/// and how it ended.
fn printed(body: []const u8, args: []const []const u8, out: []u8) !struct { text: []const u8, result: commander.Error!void } {
    const server = try outcome.FakeServer.start(.ok, body);
    defer server.stop();
    var ep: [32]u8 = undefined;
    var fds: [2]std.posix.fd_t = undefined;
    if (std.c.pipe(&fds) != 0) return error.NoPipe;
    defer _ = std.c.close(fds[0]);
    const root = try outcome.testRootTo(std.testing.allocator, try createKvCommand(std.testing.allocator), fds[1]);
    defer root.deinit();
    var full: [16][]const u8 = undefined;
    @memcpy(full[0..args.len], args);
    full[args.len] = "--endpoint";
    full[args.len + 1] = server.endpoint(&ep);
    const result = root.executeSlice(full[0 .. args.len + 2]);
    _ = std.c.close(fds[1]);
    var n: usize = 0;
    while (true) {
        const got = std.c.read(fds[0], out[n..].ptr, out.len - n);
        if (got <= 0) break;
        n += @intCast(got);
    }
    return .{ .text = out[0..n], .result = result };
}

fn answer(comptime name: []const u8, v: kv_ops.op(name).response.Value, buf: []u8) []const u8 {
    return kv_ops.op(name).response.encodeInto(buf, v) catch unreachable;
}

test "kv: an answer prints by its op: raw, record, a table, hex, JSON" {
    var b: [256]u8 = undefined;
    var o: [512]u8 = undefined;
    var r = try printed(answer("kv.get", .{ .version = 3, .value = "hello", .expires_at_ms = null }, &b), &.{ "flo", "kv", "get", "k" }, &o);
    try r.result;
    try std.testing.expectEqualStrings("hello\n", r.text);

    r = try printed(answer("kv.get", .{ .version = 3, .value = "hello", .expires_at_ms = 1700000000000 }, &b), &.{ "flo", "kv", "get", "k", "-o", "json" }, &o);
    try r.result;
    try std.testing.expectEqualStrings("{\"version\":3,\"value\":\"hello\",\"expires_at_ms\":1700000000000}\n", r.text);

    r = try printed(answer("kv.txn.commit", .{ .commit_index = 9, .ops = 2 }, &b), &.{ "flo", "kv", "txn", "commit", "5" }, &o);
    try r.result;
    try std.testing.expectEqualStrings("commit_index: 9\nops: 2\n", r.text);

    // A u64 past 2^53 is a string, so jq and JavaScript don't round it.
    r = try printed(answer("kv.txn.begin", .{ .txn = 850505347633302, .pinned = 6989541190140902256 }, &b), &.{ "flo", "kv", "txn", "begin", "rk", "-o", "json" }, &o);
    try r.result;
    try std.testing.expectEqualStrings("{\"txn\":850505347633302,\"pinned\":\"6989541190140902256\"}\n", r.text);

    r = try printed(answer("kv.expire", .{}, &b), &.{ "flo", "kv", "expire", "k", "--ttl-ms", "1h" }, &o);
    try r.result;
    try std.testing.expectEqualStrings("OK\n", r.text);

    // An incr in a transaction has no sum yet.
    r = try printed(answer("kv.incr", .{ .version = null, .value = null }, &b), &.{ "flo", "kv", "incr", "c", "--txn", "5" }, &o);
    try r.result;
    try std.testing.expectEqualStrings("OK\n", r.text);

    const Names = kv_ops.op("kv.scan").response.field_list[0].T;
    var nb: [64]u8 = undefined;
    var names = Names.Builder.init(&nb);
    _ = try names.add("a");
    _ = try names.add("b");
    r = try printed(answer("kv.scan", .{ .names = names.value(), .cursor = "\x03\x00" }, &b), &.{ "flo", "kv", "scan" }, &o);
    try r.result;
    try std.testing.expectEqualStrings("a\nb\ncursor: 0300\n", r.text);
    r = try printed(answer("kv.scan", .{ .names = names.value(), .cursor = "\x03\x00" }, &b), &.{ "flo", "kv", "scan", "-o", "json" }, &o);
    try r.result;
    try std.testing.expectEqualStrings("{\"names\":[\"a\",\"b\"],\"cursor\":\"0300\"}\n", r.text);
}

test "kv: exists exits 1 when the key isn't there, and a cut answer is a transport failure" {
    var b: [64]u8 = undefined;
    var o: [256]u8 = undefined;
    var r = try printed(answer("kv.exists", .{ .exists = false, .version = 0 }, &b), &.{ "flo", "kv", "exists", "k" }, &o);
    try std.testing.expectError(error.NotFound, r.result);
    try std.testing.expectEqualStrings("exists: false\nversion: 0\n", r.text);
    r = try printed(answer("kv.exists", .{ .exists = true, .version = 4 }, &b), &.{ "flo", "kv", "exists", "k" }, &o);
    try r.result;

    const cut = answer("kv.get", .{ .version = 3, .value = "hello", .expires_at_ms = null }, &b);
    r = try printed(cut[0 .. cut.len - 1], &.{ "flo", "kv", "get", "k" }, &o);
    try std.testing.expectError(error.Transport, r.result);
}
