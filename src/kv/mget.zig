//! kv.mget across shards: the keys grouped by the shard that owns each,
//! and the owners' answers merged back in the order the keys were asked.
//! Each owner reads its own map; no shard reads another's.

const std = @import("std");
const Allocator = std.mem.Allocator;
const table_run = @import("../node/table_run.zig");
const schema = @import("../protocol/schema.zig");

const Op = table_run.op("kv.mget");
pub const Request = Op.request;
pub const Response = Op.response;
pub const Keys = Request.field_list[0].T;
pub const Entries = Response.field_list[0].T;

/// At most one piece per shard.
pub const MAX_PIECES = @import("../config/server.zig").MAX_SHARDS;

/// Where a key's answer comes from: the piece that asked its owner, and the
/// key's place among that piece's keys.
pub const Slot = struct { piece: u16, pos: u16 };

/// One mget's keys split by owner. Pieces are numbered in the order their
/// owner first appears among the keys.
pub const Plan = struct {
    /// Per key, in the order asked.
    slots: []Slot,
    /// Per piece: the shard that owns its keys.
    owners: []u16,
    /// Per piece: its keys, in the order asked.
    keys: [][]const []const u8,

    pub fn deinit(self: *Plan, allocator: Allocator) void {
        for (self.keys) |k| allocator.free(k);
        allocator.free(self.keys);
        allocator.free(self.owners);
        allocator.free(self.slots);
    }
};

/// Split `keys` by the shard `owner(ctx, key)` names. The key slices
/// borrow from `keys`.
pub fn plan(allocator: Allocator, keys: Keys.Value, ctx: anytype, comptime owner: fn (@TypeOf(ctx), []const u8) u16) !Plan {
    const slots = try allocator.alloc(Slot, keys.count);
    errdefer allocator.free(slots);
    var owners: std.ArrayListUnmanaged(u16) = .empty;
    errdefer owners.deinit(allocator);
    var lists: std.ArrayListUnmanaged(std.ArrayListUnmanaged([]const u8)) = .empty;
    errdefer {
        for (lists.items) |*l| l.deinit(allocator);
        lists.deinit(allocator);
    }

    var it = keys.iterator();
    var i: usize = 0;
    while (try it.next()) |key| : (i += 1) {
        const shard = owner(ctx, key);
        const piece = for (owners.items, 0..) |o, p| {
            if (o == shard) break p;
        } else blk: {
            try owners.append(allocator, shard);
            try lists.append(allocator, .empty);
            break :blk owners.items.len - 1;
        };
        slots[i] = .{ .piece = @intCast(piece), .pos = @intCast(lists.items[piece].items.len) };
        try lists.items[piece].append(allocator, key);
    }

    const out = try allocator.alloc([]const []const u8, lists.items.len);
    var done: usize = 0;
    errdefer {
        for (out[0..done]) |k| allocator.free(k);
        allocator.free(out);
    }
    for (lists.items) |*l| {
        out[done] = try l.toOwnedSlice(allocator);
        done += 1;
    }
    lists.deinit(allocator);
    return .{ .slots = slots, .owners = try owners.toOwnedSlice(allocator), .keys = out };
}

/// The body of the kv.mget that asks a piece's owner for `keys`: never
/// longer than the client's, which held these keys and more.
pub fn pieceBody(buf: []u8, keys: []const []const u8) ![]const u8 {
    var w: schema.Writer = .{ .buf = buf };
    try Keys.encodeSlice(&w, keys);
    // No routing key: the owner looks each key up on its own.
    try Request.field_list[1].T.encode(&w, null);
    return w.written();
}

/// The mget's answer from its pieces' answers (`answers[p]` answers piece
/// p's keys): entries in the order asked, as many as fit `buf`. `next` is
/// the first key left out, by a piece that didn't answer all its keys or by
/// `buf`, so the client asks again from there.
pub fn merge(buf: []u8, slots: []const Slot, answers: []const Response.Value) error{ Corrupt, TooManyPieces }!Response.Value {
    if (answers.len > MAX_PIECES) return error.TooManyPieces;
    var its: [MAX_PIECES]Entries.Iterator = undefined;
    var taken: [MAX_PIECES]u16 = @splat(0);
    for (answers, 0..) |a, p| its[p] = a.entries.iterator();

    var out = Entries.Builder.init(buf);
    for (slots, 0..) |slot, k| {
        const p = slot.piece;
        if (p >= answers.len) return error.Corrupt;
        // A piece answers its keys in the order given, so a key's place is
        // the number taken from it so far.
        if (slot.pos != taken[p]) return error.Corrupt;
        if (slot.pos >= answers[p].entries.count) return .{ .entries = out.value(), .next = @intCast(k) };
        const entry = (try its[p].next()) orelse return error.Corrupt;
        taken[p] += 1;
        if (!(out.add(entry) catch return error.Corrupt)) return .{ .entries = out.value(), .next = @intCast(k) };
    }
    return .{ .entries = out.value(), .next = null };
}

/// What a gather keeps of an mget while its parts are out: where each key's
/// answer comes from. The keys themselves are gone with the request.
pub const Gather = struct {
    slots: []Slot,

    pub fn deinit(self: *Gather, allocator: Allocator) void {
        allocator.free(self.slots);
    }
};

/// A part's answer as the body its owner would send: what a gather holds
/// for the part this shard answers itself.
pub fn encodeBody(allocator: Allocator, v: Response.Value) ![]u8 {
    const size = try Response.encodedSize(v);
    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);
    const body = try Response.encodeInto(buf, v);
    std.debug.assert(body.len == size);
    return buf;
}

/// The mget's answer from its parts' bodies, merged in `buf` as `merge`
/// does. A body that does not decode is a server bug: `Corrupt`.
pub fn mergeBodies(buf: []u8, slots: []const Slot, bodies: []const ?[]u8) error{ Corrupt, TooManyPieces }!Response.Value {
    if (bodies.len > MAX_PIECES) return error.TooManyPieces;
    var answers: [MAX_PIECES]Response.Value = undefined;
    for (bodies, 0..) |b, p| {
        var diag: schema.Diagnostic = .{};
        answers[p] = Response.decodeAll(b orelse return error.Corrupt, &diag) catch return error.Corrupt;
    }
    return merge(buf, slots, answers[0..bodies.len]);
}

// =============================================================================
// Tests
// =============================================================================

fn testKeys(buf: []u8, keys: []const []const u8) !Keys.Value {
    const body = try pieceBody(buf, keys);
    var diag: schema.Diagnostic = .{};
    return (try Request.decodeAll(body, &diag)).keys;
}

/// Owner by the key's first byte: 'a' → 0, 'b' → 1, …
fn byFirstByte(_: void, key: []const u8) u16 {
    return key[0] - 'a';
}

test "mget: keys are split by owner, in the order asked" {
    var kbuf: [256]u8 = undefined;
    const keys = try testKeys(&kbuf, &.{ "b1", "a1", "b2", "c1", "a2" });
    var p = try plan(std.testing.allocator, keys, {}, byFirstByte);
    defer p.deinit(std.testing.allocator);

    try std.testing.expectEqualSlices(u16, &.{ 1, 0, 2 }, p.owners);
    try std.testing.expectEqual(@as(usize, 2), p.keys[0].len);
    try std.testing.expectEqualStrings("b2", p.keys[0][1]);
    try std.testing.expectEqualStrings("a2", p.keys[1][1]);
    try std.testing.expectEqualStrings("c1", p.keys[2][0]);
    const want = [_]Slot{ .{ .piece = 0, .pos = 0 }, .{ .piece = 1, .pos = 0 }, .{ .piece = 0, .pos = 1 }, .{ .piece = 2, .pos = 0 }, .{ .piece = 1, .pos = 1 } };
    try std.testing.expectEqualSlices(Slot, &want, p.slots);
}

fn testAnswer(buf: []u8, entries: []const Entries.Element.Value, next: ?u16) !Response.Value {
    var b = Entries.Builder.init(buf);
    for (entries) |e| try std.testing.expect(try b.add(e));
    return .{ .entries = b.value(), .next = next };
}

fn expectEntries(got: Response.Value, want: []const []const u8) !void {
    var it = got.entries.iterator();
    for (want) |w| {
        const e = (try it.next()).?;
        if (w.len == 0) {
            try std.testing.expect(!e.found);
        } else {
            try std.testing.expect(e.found);
            try std.testing.expectEqualStrings(w, e.value);
        }
    }
    try std.testing.expectEqual(@as(?Entries.Element.Value, null), try it.next());
}

test "mget: answers merge back in the order asked" {
    var kbuf: [256]u8 = undefined;
    var p = try plan(std.testing.allocator, try testKeys(&kbuf, &.{ "b1", "a1", "b2", "c1", "a2" }), {}, byFirstByte);
    defer p.deinit(std.testing.allocator);

    var abuf: [3][256]u8 = undefined;
    const answers = [_]Response.Value{
        try testAnswer(&abuf[0], &.{ .{ .found = true, .version = 4, .value = "B1" }, .{ .found = false, .version = 0, .value = "" } }, null),
        try testAnswer(&abuf[1], &.{ .{ .found = true, .version = 1, .value = "A1" }, .{ .found = true, .version = 2, .value = "A2" } }, null),
        try testAnswer(&abuf[2], &.{.{ .found = true, .version = 9, .value = "C1" }}, null),
    };
    var out: [512]u8 = undefined;
    const got = try merge(&out, p.slots, &answers);
    try std.testing.expectEqual(@as(?u16, null), got.next);
    try expectEntries(got, &.{ "B1", "A1", "", "C1", "A2" });
}

test "mget: next is the first key a piece or the answer left out" {
    var kbuf: [256]u8 = undefined;
    var p = try plan(std.testing.allocator, try testKeys(&kbuf, &.{ "a1", "b1", "a2", "b2" }), {}, byFirstByte);
    defer p.deinit(std.testing.allocator);
    var abuf: [2][256]u8 = undefined;
    var out: [512]u8 = undefined;

    // Owner b answered only its first key: the answer stops at "b2", the
    // fourth key, after taking "a2".
    const short_b = [_]Response.Value{
        try testAnswer(&abuf[0], &.{ .{ .found = true, .version = 1, .value = "A1" }, .{ .found = true, .version = 1, .value = "A2" } }, null),
        try testAnswer(&abuf[1], &.{.{ .found = true, .version = 1, .value = "B1" }}, 1),
    };
    const got = try merge(&out, p.slots, &short_b);
    try std.testing.expectEqual(@as(?u16, 3), got.next);
    try expectEntries(got, &.{ "A1", "B1", "A2" });

    // Every piece answered all, but the answer has room for two entries.
    const whole = [_]Response.Value{
        try testAnswer(&abuf[0], &.{ .{ .found = true, .version = 1, .value = "A1" }, .{ .found = true, .version = 1, .value = "A2" } }, null),
        try testAnswer(&abuf[1], &.{ .{ .found = true, .version = 1, .value = "B1" }, .{ .found = true, .version = 1, .value = "B2" } }, null),
    };
    var small: [2 + 2 * (1 + 1 + 8 + 4 + 2)]u8 = undefined;
    const cut = try merge(&small, p.slots, &whole);
    try std.testing.expect(cut.next != null);
    try std.testing.expect(cut.entries.count == cut.next.?);
}
