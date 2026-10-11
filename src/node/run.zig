//! Who may run an op, decided once. Every path that runs a request — a
//! client's, one forwarded from another shard or node, a write held for a
//! leader, a walk across shards — gets an `Authorized` from `authorize`
//! before anything runs, and the dispatcher runs nothing without one.

const std = @import("std");
const proto = @import("../protocol/proto.zig");

/// Who is asking. Every connection is anonymous until clients authenticate.
pub const Principal = union(enum) {
    anonymous,
};

/// Where a request came from, which decides the checks it has already had.
pub const Origin = enum {
    /// A client's socket.
    client,
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

/// Whether `principal` may run `req`. Everyone may, until clients
/// authenticate; then this is the one function that changes.
pub fn authorize(principal: Principal, origin: Origin, req: proto.Request) union(enum) { ok: Authorized, refused: Refused } {
    return .{ .ok = .{ .op = req.header.op_code, .principal = principal, .origin = origin, .seal = .{} } };
}

/// What makes an Authorized without `authorize`, or reaches a handler
/// around `Dispatcher.dispatch`: matched with whitespace removed, so
/// spacing doesn't hide one. `.handlers` is allowed only in dispatcher.zig.
const forgeries = [_][]const u8{
    ".seal=",
    "Seal{.",
    "Authorized{.",
    "Authorized=undefined",
    "zeroes(run.Authorized)",
    "zeroes(run_mod.Authorized)",
    "zeroes(Authorized)",
};

/// The line of the first forgery in `source`, or null. `handlers_ok`:
/// the file may touch the dispatcher's handler table (dispatcher.zig).
fn forgery(allocator: std.mem.Allocator, source: []const u8, handlers_ok: bool) !?usize {
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
    if (!handlers_ok) {
        var from: usize = 0;
        while (std.mem.indexOfPos(u8, text, from, ".handlers")) |k| : (from = k + 1) {
            const after = k + ".handlers".len;
            if (after == text.len or !(std.ascii.isAlphanumeric(text[after]) or text[after] == '_')) return lineOf(source, at[k]);
        }
    }
    return null;
}

fn lineOf(source: []const u8, index: usize) usize {
    return std.mem.count(u8, source[0..index], "\n") + 1;
}

test "run: each way of forging an Authorized or skipping dispatch is caught" {
    const forged = [_][]const u8{
        "const a: run.Authorized = .{ .op = 1, .principal = .anonymous, .origin = .client, .seal = .{} };",
        "x.seal= .{};",
        "var a: run.Authorized = undefined;",
        "var a: Authorized=   undefined;",
        "const a = std.mem.zeroes( run.Authorized );",
        "const a = run.Authorized { .op = 1 };",
        "shard.dispatcher.handlers[op].?(s, c, req);",
        "const h = d.handlers;",
    };
    for (forged) |src| try std.testing.expect((try forgery(std.testing.allocator, src, false)) != null);
    try std.testing.expectEqual(@as(?usize, null), try forgery(std.testing.allocator, "self.handlers[idx] = handler;", true));
    try std.testing.expectEqual(@as(?usize, null), try forgery(std.testing.allocator, "const auth = run.authorize(.anonymous, .client, req).ok; d.handlers_count += 1;", false));
    try std.testing.expectEqual(@as(?usize, 3), try forgery(std.testing.allocator, "a\nb\nx.seal = y;", false));
    // A function that returns one, and a field merely named like one, are fine.
    try std.testing.expectEqual(@as(?usize, null), try forgery(std.testing.allocator, "fn f() run.Authorized {\n    return x;\n}\nw.commit_index_at_seal = 1;", false));
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
            if (try forgery(a, source, std.mem.eql(u8, path, "src/node/dispatcher.zig"))) |line| {
                std.debug.print("{s}:{d}: forges an Authorized or reaches a handler around dispatch; use run.authorize and Dispatcher.dispatch\n", .{ path, line });
                found += 1;
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), found);
    try std.testing.expect(files > 100);
}
