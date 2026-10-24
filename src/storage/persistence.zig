//! Shared persistence interface for durable handlers.
//!
//! `ReplayRegistry` maps an entry type to its applier: the one function that
//! mutates a subsystem's state from a committed entry, whether that entry was
//! written here, replicated from a peer, or read back from a segment at boot.
//! `proposeEntry` builds a command entry and proposes it; the caller parks
//! the client on it, and the client is answered from what the applier
//! recorded in its `Result` once the entry has applied. Nothing waits for a commit: a producer that needs the
//! applied state picks it up once the entry applies.
//!
//! ## Usage
//!
//! Handler registration (in Shard.init, before segment replay):
//!   handler.registerReplay(&replay_registry);
//!
//! Client write: propose, then park with the op's answer function, which
//! reads only the entry's `Result`:
//!   const p = try persistence.proposeEntry(shard, .action_register, Flags.NONE, namespace, key, value);
//!   shard.park(conn, req, p, answerRegister);
//!

const std = @import("std");
const entry_mod = @import("ual/entry.zig");
const router = @import("../node/router.zig");
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const ns_keys = @import("../namespace/handler.zig");

const EntryType = entry_mod.EntryType;
const Entry = entry_mod.Entry;
const CommandPayload = entry_mod.CommandPayload;
const Flags = entry_mod.Flags;

pub const MAX_PERSIST_PAYLOAD: usize = @import("../protocol/limits.zig").MAX_PERSIST_PAYLOAD;

/// Longest key proposeEntry accepts, the same bound KV puts on its qualified
/// keys. Streams, series, queues, groups, actions and workflows propose
/// through here, so they get it even where they don't check their own.
pub const MAX_ENTRY_KEY: usize = ns_keys.MAX_QUALIFIED_KEY;

// ═══════════════════════════════════════════════════════════════════════════════
// ReplayRegistry
// ═══════════════════════════════════════════════════════════════════════════════

/// Why an applier could not apply an entry. Not a refusal: a refused
/// entry applied (it changed nothing, the same way on every node) and its
/// `Result` says why.
pub const ApplyError = error{
    /// This node could not hold what the entry adds; another node may.
    OutOfMemory,
    /// This node could not write what the entry needs to disk.
    Io,
    /// The entry breaks what every node holds true of the log: a payload
    /// that will not decode, a step no prepared write can ask for. Every
    /// node that applies it fails the same way.
    Halt,
};

/// What applying one entry came to, for the client waiting on its index:
/// an answer body, or a refusal. The applier writes it; nothing reads the
/// applier's state afterwards to rebuild it.
pub const Result = struct {
    index: u64 = 0,
    timestamp_ns: u64 = 0,
    len: u16 = 0,
    /// A write didn't fit. The answer is lost rather than cut: a cut key
    /// would name something else.
    overflowed: bool = false,
    bytes: [RESULT_MAX]u8 = undefined,
    refused_status: ?proto.StatusCode = null,
    refused_reason: proto.Reason = .unclassified,
    /// Whether a refused entry took effect: one can be refused and still
    /// change state (a processing job stored as failed).
    refused_ran: proto.Ran = .no,
    message_len: u16 = 0,
    message: [MESSAGE_MAX]u8 = undefined,

    /// The longest answer an applier records: one key, at most the longest
    /// an entry carries, and a few numbers.
    pub const RESULT_MAX = MAX_ENTRY_KEY + 64;
    /// The longest refusal message kept; a longer one is cut.
    pub const MESSAGE_MAX = 256;

    pub fn start(entry: *const Entry) Result {
        return .{ .index = entry.header.index, .timestamp_ns = entry.header.timestamp_ns };
    }

    /// Ready for `entry`'s apply, in place: the buffers aren't touched.
    pub fn reset(self: *Result, entry: *const Entry) void {
        self.index = entry.header.index;
        self.timestamp_ns = entry.header.timestamp_ns;
        self.len = 0;
        self.overflowed = false;
        self.refused_status = null;
        self.refused_ran = .no;
        self.message_len = 0;
    }

    /// The entry applied and changed nothing: what the client is told.
    pub fn refuse(self: *Result, status: proto.StatusCode, reason: proto.Reason, message: []const u8) void {
        self.refuseRan(status, reason, .no, message);
    }

    /// `refuse`, for an entry that took effect all the same.
    pub fn refuseRan(self: *Result, status: proto.StatusCode, reason: proto.Reason, ran: proto.Ran, message: []const u8) void {
        const n = @min(message.len, MESSAGE_MAX);
        @memcpy(self.message[0..n], message[0..n]);
        self.message_len = @intCast(n);
        self.refused_status = status;
        self.refused_reason = reason;
        self.refused_ran = ran;
    }

    /// `refuse`, with the message formatted.
    pub fn refuseFmt(self: *Result, status: proto.StatusCode, reason: proto.Reason, comptime fmt: []const u8, args: anytype) void {
        var buf: [MESSAGE_MAX]u8 = undefined;
        self.refuse(status, reason, std.fmt.bufPrint(&buf, fmt, args) catch buf[0..]);
    }

    /// The refusal, if the entry was refused; it points into this result.
    pub fn refused(self: *const Result) ?Refused {
        const status = self.refused_status orelse return null;
        return .{ .status = status, .refusal = .{ .reason = self.refused_reason, .ran = self.refused_ran, .message = self.message[0..self.message_len] } };
    }

    pub fn putInt(self: *Result, comptime T: type, v: T) void {
        if (!self.room(@sizeOf(T))) return;
        std.mem.writeInt(T, self.bytes[self.len..][0..@sizeOf(T)], v, .little);
        self.len += @sizeOf(T);
    }

    /// `b`, length-prefixed, whole or not at all.
    pub fn putBytes(self: *Result, b: []const u8) void {
        if (!self.room(2 + b.len)) return;
        self.putInt(u16, @intCast(b.len));
        @memcpy(self.bytes[self.len..][0..b.len], b);
        self.len += @intCast(b.len);
    }

    fn room(self: *Result, n: usize) bool {
        if (self.overflowed or RESULT_MAX - self.len < n) {
            self.overflowed = true;
            return false;
        }
        return true;
    }

    pub fn body(self: *const Result) []const u8 {
        return self.bytes[0..self.len];
    }

    /// Reads back what `putInt`/`putBytes` wrote, in order.
    pub const Reader = struct {
        bytes: []const u8,
        at: usize = 0,

        pub fn int(self: *Reader, comptime T: type) ?T {
            if (self.bytes.len - self.at < @sizeOf(T)) return null;
            defer self.at += @sizeOf(T);
            return std.mem.readInt(T, self.bytes[self.at..][0..@sizeOf(T)], .little);
        }

        pub fn slice(self: *Reader) ?[]const u8 {
            const n = self.int(u16) orelse return null;
            if (self.bytes.len - self.at < n) return null;
            defer self.at += n;
            return self.bytes[self.at..][0..n];
        }
    };

    pub fn reader(self: *const Result) Reader {
        return .{ .bytes = self.body() };
    }
};

/// A refusal and the status it goes with.
pub const Refused = struct { status: proto.StatusCode, refusal: proto.Refusal };

/// An applier: called for every committed entry of a registered type, on
/// every node, live or at boot. It records the entry's answer in `result`.
pub const ReplayFn = *const fn (ctx: *anyopaque, entry: *const Entry, result: *Result) ApplyError!void;

/// Maps EntryType → applier. Handlers register their owned entry types
/// during init; every committed entry of such a type is dispatched here.
pub const ReplayRegistry = struct {
    callbacks: [256]?ReplayEntry = @as([256]?ReplayEntry, @splat(null)),
    /// A type registered twice. The first applier is kept; the shard
    /// refuses to start, since one of the two would never run.
    duplicate: ?EntryType = null,

    const ReplayEntry = struct {
        ctx: *anyopaque,
        func: ReplayFn,
    };

    /// Register a handler for a specific entry type.
    pub fn register(self: *ReplayRegistry, etype: EntryType, ctx: *anyopaque, func: ReplayFn) void {
        const slot = &self.callbacks[@backingInt(etype)];
        if (slot.* != null) {
            if (self.duplicate == null) self.duplicate = etype;
            return;
        }
        slot.* = .{ .ctx = ctx, .func = func };
    }

    pub fn has(self: *const ReplayRegistry, etype: EntryType) bool {
        return self.callbacks[@backingInt(etype)] != null;
    }

    /// Dispatch an entry to its registered handler. False when its type
    /// has none.
    pub fn dispatch(self: *const ReplayRegistry, entry: *const Entry, result: *Result) ApplyError!bool {
        const cb = self.callbacks[entry.header.entry_type] orelse return false;
        try cb.func(cb.ctx, entry, result);
        return true;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// proposeEntry
// ═══════════════════════════════════════════════════════════════════════════════

pub const ProposeResult = @import("../raft/types.zig").ProposeResult;

/// Propose a key-value command through Raft: a CommandPayload
/// (namespace_hash + key + value) in the log, not yet committed or
/// applied. The caller parks on the result or, for a producer that does
/// not read the outcome, moves on. The leader stamps the entry; the
/// result carries the stamp for a responder that answers from it.
///
/// `shard` is `anytype` to avoid a circular import with node/shard.zig.
pub fn proposeEntry(
    shard: anytype,
    entry_type: EntryType,
    flags: u16,
    namespace: []const u8,
    key: []const u8,
    value: []const u8,
) !ProposeResult {
    if (key.len > MAX_ENTRY_KEY) return error.KeyTooLarge;
    const ns_hash = router.namespaceHash(namespace);

    // The shard's own buffer: one on the stack is filled on every call in
    // a safe build.
    const payload_buf = shard.entry_buf;
    const cmd = CommandPayload{
        .namespace_hash = ns_hash,
        .key_length = @intCast(key.len),
        .value_length = @intCast(value.len),
        .key = key,
        .value = value,
    };
    const payload_len = cmd.serialize(payload_buf) orelse return error.PayloadTooLarge;
    return shard.raft_node.propose(entry_type, flags, payload_buf[0..payload_len]);
}

/// What a client is told when its write cannot be proposed. The Raft
/// outcomes are retryable and say so; a write too large to encode is the
/// request's fault and fails the same way every time; anything else is the
/// server's.
pub fn failureStatus(err: anyerror) proto.StatusCode {
    return failureCode(err).toStatus();
}

/// The same, for handlers that answer with a `CommandResult`.
pub fn failureCode(err: anyerror) result_mod.CommandResult.ErrorCode {
    return switch (err) {
        error.NotLeader, error.WritesStopped, error.Halted => .unavailable,
        error.Overloaded => .overloaded,
        error.PayloadTooLarge, error.KeyTooLarge => .invalid_request,
        else => .internal_error,
    };
}

/// A write committed but this node did not apply it (its applier refused
/// it, or the entry could not be read back): resending it writes it twice.
pub const COMMITTED_NOT_APPLIED = "internal error: write committed but not applied on this node — do not resend";
pub const COMMITTED_REFUSAL: proto.Refusal = .{ .reason = .committed_not_applied, .ran = .yes, .message = COMMITTED_NOT_APPLIED };
/// A write committed and applied, but its answer could not be built.
pub const ANSWER_LOST = "internal error: write committed but its answer was lost — do not resend";
pub const ANSWER_LOST_REFUSAL: proto.Refusal = .{ .reason = .answer_lost, .ran = .yes, .message = ANSWER_LOST };

/// A write that couldn't be proposed, as a refusal: `propose` fails before
/// the entry reaches the log (or takes it back out), so it never ran.
pub fn failureRefusal(err: anyerror, fallback: []const u8) proto.Refusal {
    return .{ .reason = .unclassified, .ran = .no, .message = failureMessage(err, fallback) };
}

/// The same, for handlers that answer with a `CommandResult`.
pub fn failureError(err: anyerror, fallback: []const u8) result_mod.CommandResult.Error {
    return .{ .code = failureCode(err), .message = failureMessage(err, fallback), .ran = .no };
}

pub fn failureMessage(err: anyerror, fallback: []const u8) []const u8 {
    return switch (err) {
        error.NotLeader => "unavailable: electing a leader — retry",
        error.WritesStopped => "unavailable: this shard stopped taking writes — a write left memory before it reached disk; retry once the node restarts",
        error.Halted => "unavailable: this shard is halted at an entry every replica fails; this write was not run. Restarting won't help — the node's /status says where it halted",
        error.Overloaded => "overloaded: too many writes waiting for commit — back off and retry",
        error.PayloadTooLarge => "bad request: too large to write — a stream append, queue message or run input takes at most 64 KiB",
        error.KeyTooLarge => std.fmt.comptimePrint("bad request: key or name too long — at most {d} bytes, namespace included", .{MAX_ENTRY_KEY}),
        else => fallback,
    };
}

test "persistence: a write too large to encode is the request's fault, not a retry" {
    // The message names the limit.
    try std.testing.expectEqual(64 * 1024, MAX_PERSIST_PAYLOAD);
    try std.testing.expectEqual(result_mod.CommandResult.ErrorCode.invalid_request, failureCode(error.PayloadTooLarge));
    try std.testing.expectEqual(proto.StatusCode.bad_request, failureStatus(error.PayloadTooLarge));
    try std.testing.expectEqualStrings("bad request: too large to write — a stream append, queue message or run input takes at most 64 KiB", failureMessage(error.PayloadTooLarge, "x"));
    // The Raft outcomes are retryable; anything else is the server's.
    try std.testing.expectEqual(proto.StatusCode.unavailable, failureStatus(error.NotLeader));
    try std.testing.expectEqual(proto.StatusCode.unavailable, failureStatus(error.WritesStopped));
    try std.testing.expectEqual(result_mod.CommandResult.ErrorCode.internal_error, failureCode(error.IndexGap));
}

test "persistence: a key over MAX_ENTRY_KEY is refused before it is proposed" {
    const FakeRaft = struct {
        pub fn propose(_: *const @This(), _: EntryType, _: u16, _: []const u8) !ProposeResult {
            return error.NotLeader;
        }
    };
    var entry_buf: [MAX_PERSIST_PAYLOAD]u8 = undefined;
    const shard = struct { raft_node: FakeRaft, entry_buf: []u8 }{ .raft_node = .{}, .entry_buf = &entry_buf };
    const long = @as([(MAX_ENTRY_KEY + 1)]u8, @splat('k'));
    try std.testing.expectError(error.KeyTooLarge, proposeEntry(&shard, .stream_append, 0, "", &long, "v"));
    try std.testing.expectError(error.NotLeader, proposeEntry(&shard, .stream_append, 0, "", long[0..MAX_ENTRY_KEY], "v"));
    try std.testing.expectEqual(proto.StatusCode.bad_request, failureStatus(error.KeyTooLarge));
}

test "persistence: a write that couldn't be proposed never ran, whatever stopped it" {
    inline for (.{ error.NotLeader, error.WritesStopped, error.Overloaded, error.PayloadTooLarge, error.KeyTooLarge, error.IndexGap }) |err| {
        try std.testing.expectEqual(proto.Ran.no, failureRefusal(err, "x").ran);
        try std.testing.expectEqual(proto.Ran.no, failureError(err, "x").refusal().ran);
    }
    try std.testing.expectEqual(proto.Ran.yes, COMMITTED_REFUSAL.ran);
    try std.testing.expectEqual(proto.Reason.committed_not_applied, COMMITTED_REFUSAL.reason);
    try std.testing.expectEqual(proto.Ran.yes, ANSWER_LOST_REFUSAL.ran);
}

test "persistence: a result takes the longest key an entry carries whole, and a write that doesn't fit is lost, never cut" {
    var r: Result = .{};
    const longest = @as([MAX_ENTRY_KEY]u8, @splat('k'));
    r.putBytes(&longest);
    r.putInt(u64, 7);
    try std.testing.expect(!r.overflowed);
    var rd = r.reader();
    try std.testing.expectEqual(MAX_ENTRY_KEY, rd.slice().?.len);
    try std.testing.expectEqual(@as(?u64, 7), rd.int(u64));
    // Past the room left: nothing of it is written, and the result says so.
    const len = r.len;
    r.putBytes(&longest);
    try std.testing.expect(r.overflowed);
    try std.testing.expectEqual(len, r.len);
}
