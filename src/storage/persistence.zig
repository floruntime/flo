//! Shared persistence interface for durable handlers.
//!
//! Provides a ReplayRegistry (entry type → handler callback) and a shared
//! persistEntry() function that builds a CommandPayload, proposes through
//! Raft, and broadcasts to cluster peers.
//!
//! ## Usage
//!
//! Handler registration (in Shard.init, before segment replay):
//!   handler.registerReplay(&replay_registry);
//!
//! Segment replay (replaces hardcoded if/else chains):
//!   replay_registry.dispatch(&entry);
//!
//! Write path (replaces per-handler boilerplate):
//!   _ = try persistence.persistEntry(shard, .action_register, Flags.NONE, namespace, key, value);
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

/// Function pointer for replay callbacks.
/// Called during segment replay for each entry whose EntryType is registered.
pub const ReplayFn = *const fn (ctx: *anyopaque, entry: *const Entry) void;

/// Maps EntryType → handler replay callback.
///
/// Handlers register their owned entry types during init. On startup,
/// replaySegments() calls dispatch() for every entry, routing it to the
/// correct handler without hardcoded type checks.
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
/// Returns the UAL index of the committed entry.
///
/// This replaces the per-handler boilerplate of building entries and calling
/// propose() directly. Handlers just provide entry_type, namespace, key, value.
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

    // Broadcast to cluster peers via raft network
    if (shard.raft_network) |rn| {
        if (shard.raft_node.log.getEntry(propose_result.index)) |committed_entry| {
            var entry_buf: [MAX_PERSIST_PAYLOAD + 64]u8 = undefined;
            if (committed_entry.serialize(&entry_buf)) |serialized_len| {
                rn.broadcastEntry(entry_buf[0..serialized_len]) catch {};
            }
        }
    }

    return propose_result.index;
}
