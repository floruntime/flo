//! Gather — a client's read that other shards answer in parts.
//!
//! A shard never reads another shard's state. A read that needs several
//! shards' data (a kv.mget whose keys several shards own, a list every
//! shard holds part of) sends each shard its part as an ordinary forwarded
//! request, answered on that shard's own thread, and keeps an entry here
//! until every part has answered. Then the op's `merge` writes one answer
//! for the client from the parts' bodies.
//!
//! Each part another shard answers holds a reply slot (`ReplyPool`) naming
//! its entry, its generation and its place; the part this shard owns is
//! answered at the start. An entry ends exactly once: when its last part
//! arrives, or when a part fails (a refusal, or no answer by the slot's
//! deadline). Parts that arrive after it ended find another generation and
//! are dropped, so a client is answered once. Nothing here needs telling
//! that a client went away: the entry ends on its parts' slots, which always
//! end, and the answer is then dropped for want of a connection.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ReplyPool = @import("reply_pool.zig").ReplyPool;
const Shard = @import("shard.zig").Shard;
const Connection = @import("connection.zig").Connection;
const ReplyTo = @import("reply_to.zig").ReplyTo;

/// Gathers one shard may have under way: the room it keeps for fan-out.
pub const GATHER_SLOTS: u16 = 256;

/// The most parts one gather has: one per shard.
pub const MAX_PARTS: u16 = @import("../config/server.zig").MAX_SHARDS;

/// What a gathered read does with its parts.
pub const Op = struct {
    /// Writes the client's answer from the parts' ok bodies, in part order
    /// (`table_run.answer` with the op's Response, or a refusal). Called at
    /// most once, only when every part answered ok. It may fail only before
    /// it has written anything: the gather then answers `internal_error`,
    /// and a merge that wrote and then failed would answer twice.
    merge: *const fn (shard: *Shard, conn: *Connection, request_id: u64, state: *anyopaque, bodies: []const []const u8) anyerror!void,
    /// Frees what the op kept. Called exactly once, however the gather ends.
    deinit: *const fn (state: *anyopaque, allocator: Allocator) void,
};

/// One part of a gathered read. A gather has one part per owner at most:
/// room is checked per part against that owner's whole share.
pub const Part = struct {
    owner: u16,
    /// For another shard, the op it runs and the key it names, when not the
    /// client's: a part may ask a different question (a namespace's id)
    /// than the client did.
    op: ?u16 = null,
    key: ?[]const u8 = null,
    /// For another shard, the body of the request it runs (the client's op
    /// with this body), copied at the send. For this shard, its own answer
    /// body, allocated by the caller and owned by the gather from the call.
    body: []const u8,
};

/// An entry's op and the state it keeps.
pub const OpContext = struct {
    op: *const Op,
    state: *anyopaque,

    pub fn deinit(self: *OpContext, allocator: Allocator) void {
        self.op.deinit(self.state, allocator);
    }
};

/// `Context` is what the merge needs beyond the parts' answers; it has
/// `deinit(*Context, Allocator)`.
pub fn GatherTable(comptime Context: type) type {
    return struct {
        const Self = @This();

        entries: []Entry,
        /// Indices of free entries (a stack).
        free: []u16,
        free_len: usize,

        pub const Entry = struct {
            active: bool = false,
            gen: u32 = 0,
            /// The client connection the answer is for, on this shard.
            fd: i32 = -1,
            conn_id: u32 = 0,
            /// Where the answer goes instead, for a request another shard
            /// forwarded here: its client is that shard's, not this one's.
            reply_to: ?ReplyTo = null,
            request_id: u64 = 0,
            /// The client's op, for a report naming it.
            op_code: u16 = 0,
            /// Each part's answer body, owned; null until it arrives.
            answers: []?[]u8 = &.{},
            arrived: u16 = 0,
            /// A part read a halted shard's state: the answer is stale.
            stale: bool = false,
            context: Context = undefined,
        };

        pub const Ticket = struct { index: u16, gen: u32 };

        /// What a part's reply slot remembers of it.
        pub const PartRef = ReplyPool.GatherPart;

        pub fn init(allocator: Allocator) !Self {
            const entries = try allocator.alloc(Entry, GATHER_SLOTS);
            errdefer allocator.free(entries);
            @memset(entries, .{});
            const free = try allocator.alloc(u16, GATHER_SLOTS);
            for (free, 0..) |*f, i| f.* = @intCast(GATHER_SLOTS - 1 - i);
            return .{ .entries = entries, .free = free, .free_len = GATHER_SLOTS };
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            for (self.entries) |*e| if (e.active) self.clear(allocator, e);
            allocator.free(self.free);
            allocator.free(self.entries);
        }

        pub fn hasRoom(self: *const Self) bool {
            return self.free_len > 0;
        }

        /// An entry for a gather of `parts` parts, owning `context` from
        /// here on (also when this fails).
        pub fn start(self: *Self, allocator: Allocator, fd: i32, conn_id: u32, request_id: u64, parts: u16, context: Context) error{ Full, OutOfMemory }!Ticket {
            var ctx = context;
            std.debug.assert(parts > 0 and parts <= MAX_PARTS);
            if (self.free_len == 0) {
                ctx.deinit(allocator);
                return error.Full;
            }
            const answers = allocator.alloc(?[]u8, parts) catch {
                ctx.deinit(allocator);
                return error.OutOfMemory;
            };
            @memset(answers, null);
            self.free_len -= 1;
            const i = self.free[self.free_len];
            const e = &self.entries[i];
            e.* = .{ .active = true, .gen = e.gen +% 1, .fd = fd, .conn_id = conn_id, .request_id = request_id, .answers = answers, .context = ctx };
            return .{ .index = i, .gen = e.gen };
        }

        /// The entry a part belongs to, while it is under way.
        pub fn get(self: *Self, ticket: Ticket) ?*Entry {
            if (ticket.index >= self.entries.len) return null;
            const e = &self.entries[ticket.index];
            if (!e.active or e.gen != ticket.gen) return null;
            return e;
        }

        /// A part's answer, owned from here on. Returns the entry once every
        /// part has answered; the caller merges, answers and calls `finish`.
        /// Null while parts are still out, or when the part is not one the
        /// entry waits for (its gather ended, or it answered already), in
        /// which case `bytes` is freed.
        pub fn arrive(self: *Self, allocator: Allocator, part: PartRef, bytes: []u8) ?*Entry {
            const e = self.get(.{ .index = part.index, .gen = part.gen }) orelse {
                allocator.free(bytes);
                return null;
            };
            if (part.part >= e.answers.len or e.answers[part.part] != null) {
                allocator.free(bytes);
                return null;
            }
            e.answers[part.part] = bytes;
            e.arrived += 1;
            return if (e.arrived == e.answers.len) e else null;
        }

        /// End an entry: its answers and context are freed, and any part
        /// still to come is dropped.
        pub fn finish(self: *Self, allocator: Allocator, e: *Entry) void {
            std.debug.assert(e.active);
            self.clear(allocator, e);
            const i: u16 = @intCast((@intFromPtr(e) - @intFromPtr(self.entries.ptr)) / @sizeOf(Entry));
            self.free[self.free_len] = i;
            self.free_len += 1;
        }

        pub fn inUse(self: *const Self) usize {
            return GATHER_SLOTS - self.free_len;
        }

        fn clear(self: *Self, allocator: Allocator, e: *Entry) void {
            _ = self;
            for (e.answers) |a| if (a) |b| allocator.free(b);
            allocator.free(e.answers);
            e.context.deinit(allocator);
            e.answers = &.{};
            e.active = false;
        }
    };
}

// =============================================================================
// Tests
// =============================================================================

const TestContext = struct {
    buf: []u8,
    fn deinit(self: *TestContext, allocator: Allocator) void {
        allocator.free(self.buf);
    }
};

const TestTable = GatherTable(TestContext);

fn testCtx() !TestContext {
    return .{ .buf = try std.testing.allocator.alloc(u8, 8) };
}

fn owned(s: []const u8) ![]u8 {
    return std.testing.allocator.dupe(u8, s);
}

test "gather: the entry is ready when its last part arrives, in any order" {
    const a = std.testing.allocator;
    var t = try TestTable.init(a);
    defer t.deinit(a);
    const tk = try t.start(a, 5, 1, 77, 3, try testCtx());

    try std.testing.expect(t.arrive(a, .{ .index = tk.index, .gen = tk.gen, .part = 2 }, try owned("c")) == null);
    try std.testing.expect(t.arrive(a, .{ .index = tk.index, .gen = tk.gen, .part = 0 }, try owned("a")) == null);
    const e = t.arrive(a, .{ .index = tk.index, .gen = tk.gen, .part = 1 }, try owned("b")).?;
    try std.testing.expectEqualStrings("a", e.answers[0].?);
    try std.testing.expectEqualStrings("c", e.answers[2].?);
    try std.testing.expectEqual(@as(u64, 77), e.request_id);
    t.finish(a, e);
    try std.testing.expectEqual(@as(usize, 0), t.inUse());
}

test "gather: a part repeated, or arriving after its gather ended, is dropped" {
    const a = std.testing.allocator;
    var t = try TestTable.init(a);
    defer t.deinit(a);
    const tk = try t.start(a, 5, 1, 1, 2, try testCtx());
    const p0: TestTable.PartRef = .{ .index = tk.index, .gen = tk.gen, .part = 0 };

    try std.testing.expect(t.arrive(a, p0, try owned("a")) == null);
    try std.testing.expect(t.arrive(a, p0, try owned("again")) == null);
    try std.testing.expectEqualStrings("a", t.get(tk).?.answers[0].?);

    // Ended by a failed part: the other part, late, finds no entry.
    t.finish(a, t.get(tk).?);
    try std.testing.expect(t.arrive(a, .{ .index = tk.index, .gen = tk.gen, .part = 1 }, try owned("late")) == null);

    // Its entry, reused, is not reached by the old generation.
    const tk2 = try t.start(a, 6, 2, 2, 1, try testCtx());
    try std.testing.expectEqual(tk.index, tk2.index);
    try std.testing.expect(t.arrive(a, p0, try owned("stale")) == null);
    try std.testing.expect(t.get(tk2).?.arrived == 0);
}

test "gather: a full table refuses, and frees the context it was given" {
    const a = std.testing.allocator;
    var t = try TestTable.init(a);
    defer t.deinit(a);
    for (0..GATHER_SLOTS) |_| _ = try t.start(a, 5, 1, 1, 1, try testCtx());
    try std.testing.expect(!t.hasRoom());
    try std.testing.expectError(error.Full, t.start(a, 5, 1, 1, 1, try testCtx()));
}
