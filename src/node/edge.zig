//! A follower's reads of a namespace its table doesn't have yet. Its table
//! may be behind the leader's, so before answering that the namespace
//! doesn't exist it asks the leader how far the leader's table is
//! published, waits for its own to get there, and looks again. This holds
//! those reads, and remembers for a moment the names that turned out to be
//! missing, so asking about them again doesn't load the leader.

const std = @import("std");
const ReplyTo = @import("reply_to.zig").ReplyTo;
const Origin = @import("run.zig").Origin;
const limits = @import("../protocol/limits.zig");

/// Reads held at once, each shard. Past it a read is refused overloaded,
/// not run: never that the namespace isn't found, since nothing looked.
pub const WAITS: usize = 64;
/// How long a held read waits for the leader's answer and then for this
/// node's table to catch up, before it is answered unavailable.
pub const WAIT_MS: u64 = 2000;
/// How long a name found missing is answered missing without asking again.
pub const MISS_MS: u64 = 100;
/// Names remembered missing at once, each shard.
pub const MISSES: usize = 1024;

pub const Wait = struct {
    active: bool = false,
    /// The client's own connection, on this shard.
    reply_to: ReplyTo = undefined,
    origin: Origin = .client,
    request_id: u64 = 0,
    /// The request, serialized, run once its namespace is resolved.
    bytes: []u8 = &.{},
    name: []u8 = &.{},
    /// The leader's published index, once it has answered.
    index: ?u64 = null,
    deadline_ms: u64 = 0,
};

const Miss = struct {
    len: u8 = 0,
    name: [limits.MAX_NAMESPACE_NAME]u8 = undefined,
    until_ms: u64 = 0,
};

pub const Edge = struct {
    waits: [WAITS]Wait = @splat(.{}),
    count: u32 = 0,
    /// A ring: the oldest name is the one replaced.
    misses: ?*[MISSES]Miss = null,
    next_miss: usize = 0,

    pub fn deinit(self: *Edge, allocator: std.mem.Allocator) void {
        for (&self.waits) |*w| if (w.active) self.drop(allocator, w);
        if (self.misses) |m| allocator.destroy(m);
        self.misses = null;
    }

    pub fn free(self: *Edge) ?*Wait {
        for (&self.waits) |*w| if (!w.active) return w;
        return null;
    }

    /// The leader's index another read of `name` already has: a read that
    /// comes after waits for the same, and needs no ask of its own.
    pub fn knownIndex(self: *const Edge, name: []const u8) ?u64 {
        for (&self.waits) |*w| if (w.active and w.index != null and std.mem.eql(u8, w.name, name)) return w.index;
        return null;
    }

    /// Every held read of `name` still waiting for the leader now waits
    /// for its table to reach `index`.
    pub fn answered(self: *Edge, name: []const u8, index: u64, now_ms: u64) void {
        for (&self.waits) |*w| if (w.active and w.index == null and std.mem.eql(u8, w.name, name)) {
            w.index = index;
            w.deadline_ms = @max(w.deadline_ms, now_ms + WAIT_MS / 2);
        };
    }

    pub fn drop(self: *Edge, allocator: std.mem.Allocator, w: *Wait) void {
        allocator.free(w.bytes);
        allocator.free(w.name);
        w.* = .{};
        self.count -= 1;
    }

    /// Whether `name` was found missing within the last MISS_MS.
    pub fn missed(self: *const Edge, name: []const u8, now_ms: u64) bool {
        const misses = self.misses orelse return false;
        for (misses) |*m| if (m.len == name.len and now_ms < m.until_ms and std.mem.eql(u8, m.name[0..m.len], name)) return true;
        return false;
    }

    /// Remember `name` missing for MISS_MS. With no memory to remember it,
    /// the next read asks again.
    pub fn noteMissing(self: *Edge, allocator: std.mem.Allocator, name: []const u8, now_ms: u64) void {
        if (name.len > limits.MAX_NAMESPACE_NAME) return;
        const misses = self.misses orelse blk: {
            const m = allocator.create([MISSES]Miss) catch return;
            @memset(m, .{});
            self.misses = m;
            break :blk m;
        };
        const m = &misses[self.next_miss];
        self.next_miss = (self.next_miss + 1) % MISSES;
        m.len = @intCast(name.len);
        @memcpy(m.name[0..name.len], name);
        m.until_ms = now_ms + MISS_MS;
    }
};

test "edge: a name found missing is remembered for MISS_MS, the oldest replaced first" {
    var e: Edge = .{};
    defer e.deinit(std.testing.allocator);
    try std.testing.expect(!e.missed("a", 0));
    e.noteMissing(std.testing.allocator, "a", 1000);
    try std.testing.expect(e.missed("a", 1000));
    try std.testing.expect(e.missed("a", 1000 + MISS_MS - 1));
    try std.testing.expect(!e.missed("a", 1000 + MISS_MS));
    try std.testing.expect(!e.missed("ab", 1000));
    for (0..MISSES) |_| e.noteMissing(std.testing.allocator, "b", 1000);
    try std.testing.expect(!e.missed("a", 1000));
}
