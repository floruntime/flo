//! Shared persistence interface for durable handlers.
//!
//! `ReplayRegistry` maps an entry type to its applier: the one function that
//! mutates a subsystem's state from a committed entry, whether that entry was
//! written here, replicated from a peer, or read back from a segment at boot.
//! `persistEntry` builds a command entry, proposes it and returns once it
//! has committed; it does not apply.
//!
//! ## Usage
//!
//! Handler registration (in Shard.init, before segment replay):
//!   handler.registerReplay(&replay_registry);
//!
//! Write path: persist, drain the committed log, then read the result from
//! handler or projection state:
//!   _ = try persistence.persistEntry(shard, .action_register, Flags.NONE, namespace, key, value);
//!   if (!shard.applyCommitted()) return error.NotApplied;
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
// persistEntry
// ═══════════════════════════════════════════════════════════════════════════════

/// Persist a key-value command through Raft for durability and replication.
///
/// Builds a CommandPayload (namespace_hash + key + value), proposes it
/// through the shard's Raft node and waits for it to commit — at once
/// alone, one round trip to a majority in a cluster, with the shard's
/// other work paused meanwhile. Returns the committed index. Does not
/// apply it.
///
/// `shard` is `anytype` to avoid a circular import with node/shard.zig.
/// It must have `.raft_node` and `awaitCommit`.
pub fn persistEntry(
    shard: anytype,
    entry_type: EntryType,
    flags: u16,
    namespace: []const u8,
    key: []const u8,
    value: []const u8,
) !u64 {
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

    const timestamp_ns: u64 = @intCast(@as(u64, @bitCast(@as(i64, @import("stdx").time.milliTimestamp()))) * 1_000_000);

    const propose_result = try shard.raft_node.propose(
        entry_type,
        flags,
        timestamp_ns,
        payload_buf[0..payload_len],
    );

    switch (shard.awaitCommit(propose_result.index)) {
        .committed => return propose_result.index,
        .leadership_lost => return error.NotCommitted,
        .timed_out => return error.CommitUnconfirmed,
    }
}

/// What a client is told when `persistEntry` fails. The Raft outcomes are
/// retryable and say so; anything else is the server's fault.
pub fn failureStatus(err: anyerror) proto.StatusCode {
    return switch (err) {
        error.NotCommitted, error.CommitUnconfirmed, error.NotLeader => .unavailable,
        else => .internal_error,
    };
}

/// The same, for handlers that answer with a `CommandResult`.
pub fn failureCode(err: anyerror) result_mod.CommandResult.ErrorCode {
    return switch (err) {
        error.NotCommitted, error.CommitUnconfirmed, error.NotLeader => .unavailable,
        else => .internal_error,
    };
}

pub fn failureMessage(err: anyerror, fallback: []const u8) []const u8 {
    return switch (err) {
        error.NotCommitted => "unavailable: lost leadership before commit — write may still apply",
        error.CommitUnconfirmed => "unavailable: commit not confirmed in time — write may still apply",
        error.NotLeader => "unavailable: electing a leader — retry",
        else => fallback,
    };
}
