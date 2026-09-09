//! Shared persistence interface for durable handlers.
//!
//! `ReplayRegistry` maps an entry type to its applier: the one function that
//! mutates a subsystem's state from a committed entry, whether that entry was
//! written here, replicated from a peer, or read back from a segment at boot.
//! `persistEntry` builds a command entry, proposes it and broadcasts it; it
//! does not apply.
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
/// through the shard's Raft node, and broadcasts to cluster peers.
/// Returns the index of the committed entry. Does not apply it.
///
/// `shard` is `anytype` to avoid a circular import with node/shard.zig.
/// It must have `.raft_node` and `.raft_network` fields.
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

    // Broadcast to cluster peers via raft network.
    //
    // Use getEntryCopy, not getEntry: the zero-copy getEntry returns null for an
    // entry whose payload wraps the hot-ring byte boundary, which would silently
    // drop the entry from the broadcast and diverge followers. payload_buf is
    // free to reuse here — propose() already copied it into the ring.
    if (shard.raft_network) |rn| {
        if (shard.raft_node.log.getEntryCopy(propose_result.index, &payload_buf)) |committed_entry| {
            var entry_buf: [MAX_PERSIST_PAYLOAD + 64]u8 = undefined;
            if (committed_entry.serialize(&entry_buf)) |serialized_len| {
                rn.broadcastEntry(entry_buf[0..serialized_len]) catch {};
            }
        }
    }

    return propose_result.index;
}
