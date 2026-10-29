//! Who may run an op, decided once. Every path that runs a request — a
//! client's, one forwarded from another shard or node, a write held for a
//! leader, a walk across shards — gets an `Authorized` from `authorize`
//! before anything runs, and the dispatcher runs nothing without one.

const std = @import("std");
const proto = @import("../protocol/proto.zig");
const table = @import("../protocol/table.zig");

/// Who is asking. Every connection is anonymous until clients authenticate.
pub const Principal = union(enum) {
    anonymous,
};

/// Where a request came from, which decides the checks it has already had.
pub const Origin = enum {
    /// A client's socket.
    client,
    /// The node's admin socket: a Unix socket only its owner may connect
    /// to, so the host's file permission is the check.
    admin,
    /// Another shard of this node, which routed it.
    shard,
    /// Another node, which forwarded it to its leader here.
    peer,
    /// A write this node held until it led.
    held,
};

/// Proof that `op` was authorized for `principal`. Only `authorize` makes
/// one; a test checks no other file does.
pub const Authorized = struct {
    op: u16,
    principal: Principal,
    origin: Origin,
    seal: Seal,
};

const Seal = struct { by_authorize: void = {} };

pub const Refused = struct {
    status: proto.StatusCode,
    refusal: proto.Refusal,
};

/// Whether `principal` may run `req`. An op that administers the node
/// runs only from its admin socket, or as a request already authorized
/// at an edge: one a peer forwarded over its authenticated link, or one
/// held here for a leader. Everyone may run every other op, until clients
/// authenticate; then this is the one function that changes.
pub fn authorize(principal: Principal, origin: Origin, req: proto.Request) union(enum) { ok: Authorized, refused: Refused } {
    if (adminOnly(req.header.op_code)) switch (origin) {
        .admin, .peer, .held => {},
        .client, .shard => return .{ .refused = .{ .status = .unauthorized, .refusal = .{ .reason = .admin_only, .ran = .no, .message = ADMIN_ONLY_MESSAGE } } },
    };
    return .{ .ok = .{ .op = req.header.op_code, .principal = principal, .origin = origin, .seal = .{} } };
}

pub const ADMIN_ONLY_MESSAGE = "unauthorized: this op is served only on the node's admin socket (admin.sock in its data directory, unless [admin] socket places it elsewhere); run the command on the node itself";

/// Ops that administer the node rather than its data.
fn adminOnly(op: u16) bool {
    return op == @backingInt(proto.OpCode.cluster_repair_skip) or table.isAdmin(op);
}

/// What makes an Authorized without `authorize`, or reaches a handler
/// around `Dispatcher.dispatch`: matched with whitespace removed, so
/// spacing doesn't hide one. These are checked everywhere but run.zig; past
/// them, the name `Authorized` may appear only in the files that pass one
/// along, and the handler table only in the dispatcher.
const forgeries = [_][]const u8{
    ".seal=",
    "Seal{.",
    "Authorized{.",
    "Authorized=undefined",
    "Authorized,undefined",
    "zeroes(run.Authorized)",
    "zeroes(run_mod.Authorized)",
    "zeroes(Authorized)",
    "zeroInit(",
    "bytesToValue(run.Authorized",
    "bytesToValue(run_mod.Authorized",
    "bytesToValue(Authorized",
};

/// What a file may touch: run.zig makes Authorized values; dispatcher.zig
/// and shard.zig take and pass them; only dispatcher.zig holds the handler
/// table.
const Allowed = struct { authorized: bool, handlers: bool };

fn allowedFor(path: []const u8) Allowed {
    const dispatcher = std.mem.eql(u8, path, "src/node/dispatcher.zig");
    return .{
        .authorized = dispatcher or std.mem.eql(u8, path, "src/node/shard.zig"),
        .handlers = dispatcher,
    };
}

/// The line of the first forgery in `source`, or null.
fn forgery(allocator: std.mem.Allocator, source: []const u8, allowed: Allowed) !?usize {
    const dense = try allocator.alloc(u8, source.len);
    defer allocator.free(dense);
    const at = try allocator.alloc(u32, source.len);
    defer allocator.free(at);
    var n: usize = 0;
    for (source, 0..) |c, i| {
        if (c == ' ' or c == '\t' or c == '\n' or c == '\r') continue;
        dense[n] = c;
        at[n] = @intCast(i);
        n += 1;
    }
    const text = dense[0..n];
    for (forgeries) |needle| if (std.mem.indexOf(u8, text, needle)) |k| return lineOf(source, at[k]);
    if (!allowed.handlers) {
        if (word(text, ".handlers")) |k| return lineOf(source, at[k]);
        // `@field(d, "handlers")` reaches the table by name.
        if (std.mem.indexOf(u8, text, "\"handlers\"")) |k| return lineOf(source, at[k]);
    }
    if (!allowed.authorized) {
        // An alias, an @as, any spelling: the name itself is the tell.
        if (word(text, "Authorized")) |k| return lineOf(source, at[k]);
    }
    return null;
}

/// Where `name` first occurs not followed by more of an identifier.
fn word(text: []const u8, name: []const u8) ?usize {
    var from: usize = 0;
    while (std.mem.indexOfPos(u8, text, from, name)) |k| : (from = k + 1) {
        const before_ok = k == 0 or !(std.ascii.isAlphanumeric(text[k - 1]) or text[k - 1] == '_') or name[0] == '.';
        const after = k + name.len;
        const after_ok = after == text.len or !(std.ascii.isAlphanumeric(text[after]) or text[after] == '_');
        if (before_ok and after_ok) return k;
    }
    return null;
}

fn lineOf(source: []const u8, index: usize) usize {
    return std.mem.count(u8, source[0..index], "\n") + 1;
}

test "run: each way of forging an Authorized or skipping dispatch is caught" {
    const a = std.testing.allocator;
    const nobody: Allowed = .{ .authorized = false, .handlers = false };
    const shard: Allowed = .{ .authorized = true, .handlers = false };
    const dispatcher: Allowed = .{ .authorized = true, .handlers = true };
    // Caught even in the files that may pass an Authorized along.
    const forged_anywhere = [_][]const u8{
        "const x: run.Authorized = .{ .op = 1, .principal = .anonymous, .origin = .client, .seal = .{} };",
        "x.seal= .{};",
        "var x: run.Authorized = undefined;",
        "var x: Authorized=   undefined;",
        "const x = std.mem.zeroes( run.Authorized );",
        "const x = run.Authorized { .op = 1 };",
        "const x = std.mem.zeroInit(run.Authorized, .{});",
        "const x = @as(run.Authorized, undefined);",
        "const x = std.mem.bytesToValue(run_mod.Authorized, &buf);",
    };
    for (forged_anywhere) |src| {
        try std.testing.expect((try forgery(a, src, nobody)) != null);
        try std.testing.expect((try forgery(a, src, shard)) != null);
    }
    // Caught outside the files that pass one along: any mention of the name,
    // and any reach for the handler table.
    const forged_elsewhere = [_][]const u8{
        "const A = run.Authorized;\nvar x: A = undefined;",
        "fn f() run.Authorized {\n    return x;\n}",
        "shard.dispatcher.handlers[op].?(s, c, req);",
        "const h = d.handlers;",
        "const h = @field(d, \"handlers\");",
    };
    for (forged_elsewhere) |src| try std.testing.expect((try forgery(a, src, nobody)) != null);

    try std.testing.expectEqual(@as(?usize, null), try forgery(a, "self.handlers[idx] = handler; const h = @field(self, \"handlers\");", dispatcher));
    try std.testing.expect((try forgery(a, "const h = d.handlers;", shard)) != null);
    try std.testing.expectEqual(@as(?usize, null), try forgery(a, "const auth = run.authorize(.anonymous, .client, req).ok; d.handlers_count += 1;", nobody));
    try std.testing.expectEqual(@as(?usize, null), try forgery(a, "fn dispatchLocal(auth: run_mod.Authorized) void {\n    return x;\n}\nw.commit_index_at_seal = 1;", shard));
    try std.testing.expectEqual(@as(?usize, 3), try forgery(a, "a\nb\nx.seal = y;", nobody));
}

test "run: no file but this one makes an Authorized, and none but the dispatcher touches its handlers" {
    const stdx = @import("stdx");
    const a = std.testing.allocator;
    var stack: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (stack.items) |p| a.free(p);
        stack.deinit(a);
    }
    try stack.append(a, try a.dupe(u8, "src"));
    var i: usize = 0;
    var files: usize = 0;
    var found: usize = 0;
    while (i < stack.items.len) : (i += 1) {
        const dir_path = stack.items[i];
        var dir = try stdx.fs.openDir(dir_path, .{ .iterate = true });
        defer stdx.fs.closeDir(dir);
        var it = dir.iterate();
        while (try it.next(stdx.io.instance())) |entry| {
            const path = try std.fs.path.join(a, &.{ dir_path, entry.name });
            if (entry.kind == .directory) {
                try stack.append(a, path);
                continue;
            }
            defer a.free(path);
            if (!std.mem.endsWith(u8, entry.name, ".zig") or std.mem.eql(u8, path, "src/node/run.zig")) continue;
            const source = try stdx.fs.readFileAlloc(a, path, 4 << 20);
            defer a.free(source);
            files += 1;
            if (try forgery(a, source, allowedFor(path))) |line| {
                std.debug.print("{s}:{d}: forges an Authorized or reaches a handler around dispatch; use run.authorize and Dispatcher.dispatch\n", .{ path, line });
                found += 1;
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), found);
    try std.testing.expect(files > 100);
}

test "run: an admin op runs only from the admin socket or as a request already authorized at an edge" {
    const admin_req: proto.Request = .{ .header = proto.RequestHeader.init(.cluster_repair_skip, 1), .namespace = "", .key = "", .value = "" };
    const data_req: proto.Request = .{ .header = proto.RequestHeader.init(.cluster_status, 2), .namespace = "", .key = "", .value = "" };
    inline for (.{ Origin.admin, Origin.peer, Origin.held }) |o| try std.testing.expect(authorize(.anonymous, o, admin_req) == .ok);
    inline for (.{ Origin.client, Origin.shard }) |o| {
        const r = authorize(.anonymous, o, admin_req).refused;
        try std.testing.expectEqual(proto.StatusCode.unauthorized, r.status);
        try std.testing.expectEqual(proto.Reason.admin_only, r.refusal.reason);
    }
    inline for (@typeInfo(Origin).@"enum".field_names) |name| try std.testing.expect(authorize(.anonymous, @field(Origin, name), data_req) == .ok);
}
