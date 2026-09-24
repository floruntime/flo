//! Shared persistence interface for durable handlers.
//!
//! `ReplayRegistry` maps an entry type to its applier: the one function that
//! mutates a subsystem's state from a committed entry, whether that entry was
//! written here, replicated from a peer, or read back from a segment at boot.
//! `proposeEntry` builds a command entry and proposes it; the caller parks
//! the client on the result and answers from a responder once the entry
//! has applied. Nothing waits for a commit: a producer that needs the
//! applied state picks it up once the entry applies.
//!
//! ## Usage
//!
//! Handler registration (in Shard.init, before segment replay):
//!   handler.registerReplay(&replay_registry);
//!
//! Client write: propose, park, read the result back in the responder:
//!   const p = try persistence.proposeEntry(shard, .action_register, Flags.NONE, namespace, key, value);
//!   return .{ .parked = p };   // the dispatcher parks with the module's responder
//!

const std = @import("std");
const entry_mod = @import("ual/entry.zig");
const router = @import("../node/router.zig");
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");

const EntryType = entry_mod.EntryType;
const Entry = entry_mod.Entry;
const CommandPayload = entry_mod.CommandPayload;
const Flags = entry_mod.Flags;

pub const MAX_PERSIST_PAYLOAD: usize = 65536;

// ═══════════════════════════════════════════════════════════════════════════════
// ReplayRegistry
// ═══════════════════════════════════════════════════════════════════════════════

/// An applier: called for every committed entry of a registered type.
pub const ReplayFn = *const fn (ctx: *anyopaque, entry: *const Entry) void;

/// Maps EntryType → applier. Handlers register their owned entry types
/// during init; every committed entry of such a type is dispatched here.
pub const ReplayRegistry = struct {
    callbacks: [256]?ReplayEntry = [_]?ReplayEntry{null} ** 256,

    const ReplayEntry = struct {
        ctx: *anyopaque,
        func: ReplayFn,
    };

    /// Register a handler for a specific entry type.
    pub fn register(self: *ReplayRegistry, etype: EntryType, ctx: *anyopaque, func: ReplayFn) void {
        self.callbacks[@intFromEnum(etype)] = .{ .ctx = ctx, .func = func };
    }

    pub fn has(self: *const ReplayRegistry, etype: EntryType) bool {
        return self.callbacks[@intFromEnum(etype)] != null;
    }

    /// Dispatch an entry to its registered handler (if any).
    /// Returns true if a handler was found and called.
    pub fn dispatch(self: *const ReplayRegistry, entry: *const Entry) bool {
        if (self.callbacks[entry.header.entry_type]) |cb| {
            cb.func(cb.ctx, entry);
            return true;
        }
        return false;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// proposeEntry
// ═══════════════════════════════════════════════════════════════════════════════

pub const ProposeResult = @import("../raft/types.zig").ProposeResult;

/// Propose a key-value command through Raft: a CommandPayload
/// (namespace_hash + key + value) in the log, not yet committed or
/// applied. The caller parks on the result or, for a producer that does
/// not read the outcome, moves on.
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
    const timestamp_ns: u64 = @intCast(@as(u64, @bitCast(@as(i64, @import("stdx").time.milliTimestamp()))) * 1_000_000);
    return proposeEntryAt(shard, entry_type, flags, namespace, key, value, timestamp_ns);
}

/// `proposeEntry` with the entry's header timestamp chosen by the caller,
/// for a responder that answers from it.
pub fn proposeEntryAt(
    shard: anytype,
    entry_type: EntryType,
    flags: u16,
    namespace: []const u8,
    key: []const u8,
    value: []const u8,
    timestamp_ns: u64,
) !ProposeResult {
    const ns_hash = router.namespaceHash(namespace);

    var payload_buf: [MAX_PERSIST_PAYLOAD]u8 = undefined;
    const cmd = CommandPayload{
        .namespace_hash = ns_hash,
        .key_length = @intCast(key.len),
        .value_length = @intCast(value.len),
        .key = key,
        .value = value,
    };
    const payload_len = cmd.serialize(&payload_buf) orelse return error.PayloadTooLarge;
    return shard.raft_node.propose(entry_type, flags, timestamp_ns, payload_buf[0..payload_len]);
}

/// What a client is told when its write cannot be proposed. The Raft
/// outcomes are retryable and say so; anything else is the server's fault.
pub fn failureStatus(err: anyerror) proto.StatusCode {
    return failureCode(err).toStatus();
}

/// The same, for handlers that answer with a `CommandResult`.
pub fn failureCode(err: anyerror) result_mod.CommandResult.ErrorCode {
    return switch (err) {
        error.NotLeader => .unavailable,
        error.Overloaded => .overloaded,
        else => .internal_error,
    };
}

/// A write committed but this node did not apply it (its applier refused
/// it, or the entry could not be read back): resending it writes it twice.
pub const COMMITTED_NOT_APPLIED = "internal error: write committed but not applied on this node — do not resend";
/// A write committed and applied, but its answer could not be built.
pub const ANSWER_LOST = "internal error: write committed but its answer was lost — do not resend";

pub fn failureMessage(err: anyerror, fallback: []const u8) []const u8 {
    return switch (err) {
        error.NotLeader => "unavailable: electing a leader — retry",
        error.Overloaded => "overloaded: too many writes waiting for commit — back off and retry",
        else => fallback,
    };
}
