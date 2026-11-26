//! The memory gate: a client's write that would take its shard past the
//! share its leader bound is refused at apply, `limit`/`memory_budget`.
//!
//! What may be refused is said by entry type, at the top level only: an
//! entry a client proposed that can grow a charged family. Nothing a channel
//! carries is refused here (a sink's message is held before it is proposed,
//! `channels.sinkRoom`), nor anything the node writes for itself: a duty, a
//! delete, a channel's own entries. A type whose machine caps itself (`own`)
//! applies its own rule: a run's start, which a duty may propose, and a
//! signal are held by the runs machine (`start_verdict.zig`).
//!
//! The verdict reads only the committed store and the bound share, so every
//! replica, replay included, reaches the same one.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const view_mod = @import("../contract/view.zig");
const store_mod = @import("../contract/store.zig");
const pool = @import("pool.zig");
const settings = @import("../settings/machine.zig");
const plant = @import("../plant.zig");
const stream_machine = @import("../stream/machine.zig");

const EntryType = entry_mod.EntryType;
const Entry = entry_mod.Entry;

pub const Class = enum {
    /// Refused when it would add charged bytes past the share.
    refusable,
    /// Its machine holds it to the share itself (a group's deliver takes no
    /// fresh records past it, but still redelivers).
    own,
    /// Never refused for memory: it frees, it is the node's own, or it is
    /// charged but not held to the share yet.
    never,
};

/// Every entry type's class. A new type fails the build until it is given one.
pub fn classOf(t: EntryType) Class {
    return switch (t) {
        .kv_put, .kv_batch, .kv_incr, .kv_json_patch => .refusable,
        // A stream config that alters is admitted (`refusable`).
        .stream_append, .stream_config => .refusable,
        .queue_enqueue => .refusable,
        .ts_write, .ts_write_batch => .refusable,
        .group_create, .cg_create => .refusable,
        .action_register, .action_invoke => .refusable,
        // A replace that shrinks fits: the gate weighs the exact delta.
        .workflow_create => .refusable,
        .processing_submit => .refusable,

        .group_deliver => .own,
        // A run's start creates for a user, whoever proposed it (a client, a
        // schedule, a trigger or a parent): the runs machine holds it to the
        // share after staging it, answering a child's parent rather than
        // dropping the reply.
        .workflow_start => .own,

        // A TTL set on a live key's current value, in place: no history
        // version kept, its size unchanged. It is how a user frees memory.
        .kv_expire => .never,
        // Deletes and trims.
        .kv_delete, .stream_trim, .stream_delete, .stream_clean, .stream_forget => .never,
        .ts_delete, .ts_retention, .queue_purge, .group_delete, .action_delete, .workflow_run_delete, .action_run_delete => .never,
        .processing_stop, .processing_cancel, .processing_remove => .never,
        // Duties, each bounded by what was admitted before it.
        .kv_reap, .queue_deliver, .queue_settle, .queue_release, .queue_extend, .queue_expire, .queue_requeue, .queue_drop => .never,
        .group_settle, .group_release, .group_extend, .group_expire, .group_clean, .group_steal, .group_requeue, .group_drop => .never,
        // A run admitted is let finish: a settle refused would expire its
        // lease and run it again, its result lost. Its result and release's
        // error grow linearly in the runs admitted.
        .action_settle => .never,
        .action_deliver, .action_release, .action_extend, .action_expire, .action_abandon_send, .action_abandon => .never,
        // Shard 0 proposes a create and the write behind it, and the write
        // relies on the create: a refused create would orphan its id. The
        // table is capped instead.
        .namespace_create, .namespace_alter => .never,
        // Held by the runs machine too: past the share a signal is refused,
        // but one that wakes its run is taken within a slack.
        .workflow_signal => .own,
        // A run admitted is let finish: a step's outputs are keyed by its
        // name, so a run's are bounded by its definition. Enabling is set in
        // place.
        .workflow_complete, .workflow_enabled, .workflow_step => .never,
        // A child's start arrives in an inbound and holds itself to the share
        // there. A retry rewrites its call in place and asks again, and a
        // refused one would leave the call with nothing to ask it; the rest
        // free, or answer a call.
        .child_start, .child_refuse, .step_call_retry, .step_call_clear, .call_reply, .call_done => .never,
        .processing_sink_batch => .never,
        .settings_pool_share, .settings_node_mem, .settings_node_del, .settings_budget_clean, .settings_budget_bind, .settings_quota_bind => .never,
        // An inbound's messages are judged before they are proposed, or are
        // the node's own; an unpark moves bytes already charged.
        .channel_inbound, .channel_trim, .channel_probe, .channel_unpark => .never,
        // A run's history rides its own group's channel, and a drop frees it.
        .history_chunk, .history_index, .history_drop => .never,
        .raft_config, .raft_noop, .raft_snapshot, .repair_skip => .never,
    };
}

/// `classOf(t) == .refusable` by type byte; false for a byte no type has.
const REFUSABLE: [256]bool = blk: {
    var r: [256]bool = @splat(false);
    for (std.enums.values(EntryType)) |t| r[@backingInt(t)] = classOf(t) == .refusable;
    break :blk r;
};

/// Whether `e` may be refused for memory: its type's class is `refusable`,
/// it isn't a sink's direct write (held before it was proposed, as its
/// channel's are), and it isn't a stream alter. An alter only sets an
/// existing stream's retention, one record per stream, and is how a user
/// frees memory; a stream config that creates stays refusable.
pub fn refusable(e: *const Entry) bool {
    // Every write passes here: a table, not a search of the enum.
    if (!REFUSABLE[e.header.entry_type]) return false;
    if (e.header.flags & entry_mod.Flags.SINK != 0) return false;
    if (e.header.entry_type == @backingInt(EntryType.stream_config)) return !stream_machine.isAlter(e);
    return true;
}

/// What the gate weighed: the shard's use, what the entry adds, and the share.
pub const Over = struct { use: u64, delta: i64, share: u64 };

/// Whether the share bound on `store` holds the entry staged in `view` back:
/// it is refusable, at the top level, isn't a sink's direct write, and would
/// add charged bytes past the share. Null when it may commit.
pub fn judge(view: *view_mod.View, e: *const Entry) ?Over {
    const system = (comptime plant.active == .memory_refuses_system) and e.header.entry_type == @backingInt(EntryType.queue_deliver);
    if (!refusable(e) and !system) return null;
    if (comptime plant.active == .memory_admits_over_share) return null;
    return over(view);
}

/// `judge`'s rule for any staged writes: null unless a share is bound and
/// they would add charged bytes past it. The cheap bounds go first; the
/// range deletes are walked only when they alone could admit.
pub fn over(view: *view_mod.View) ?Over {
    const read: view_mod.ReadView = .{ .store = view.store };
    const share = (settings.boundShare(read) orelse return null).share_bytes;
    const use = pool.useOf(view.store);
    if (use +| view.stagedBound(&pool.charged, pool.RESIDENT_RECORD) <= share) return null;
    const bound = view.chargedDelta(&pool.charged, pool.RESIDENT_RECORD, false);
    if (fits(use, bound, share)) return null;
    const exact = view.chargedDelta(&pool.charged, pool.RESIDENT_RECORD, true);
    if (fits(use, exact, share)) return null;
    return .{ .use = use, .delta = exact, .share = share };
}

/// Whether what was staged in `view` since `mark` (a workflow's call of an
/// action, applied inside an inbound) would take the committed use past the
/// share: its machine answers the call rather than start it. Earlier
/// messages of the same inbound aren't counted, so an inbound passes the
/// share by at most itself.
pub fn overSince(view: *view_mod.View, mark: view_mod.Mark) ?Over {
    const share = (settings.boundShare(.{ .store = view.store }) orelse return null).share_bytes;
    const use = pool.useOf(view.store);
    const add = view.stagedBoundSince(mark, &pool.charged, pool.RESIDENT_RECORD);
    if (use +| add <= share) return null;
    return .{ .use = use, .delta = @intCast(add), .share = share };
}

/// The share bound on `store`, and its use, when its use has reached it: a
/// machine that caps itself (`own`) takes nothing new then. Null when no
/// share is bound or the use is under it.
pub fn atShare(store: *store_mod.Store) ?Over {
    const share = (settings.boundShare(.{ .store = store }) orelse return null).share_bytes;
    const use = pool.useOf(store);
    if (use < share) return null;
    return .{ .use = use, .delta = 0, .share = share };
}

/// Whether `delta` on top of `use` stays within `share`: anything that adds
/// nothing does.
pub fn fits(use: u64, delta: i64, share: u64) bool {
    if (delta <= 0) return true;
    return use +| @as(u64, @intCast(delta)) <= share;
}

/// Refuses the entry `o` holds back, in `out`, saying why.
pub fn refuse(out: *persistence.Result, shard: ?u16, o: Over) void {
    // A refusal answers with its reason alone.
    out.len = 0;
    out.overflowed = false;
    if (shard) |s| {
        out.refuseFmt(.limit, .memory_budget, "shard {d} memory: {d} + {d} would pass its share {d}", .{ s, o.use, o.delta, o.share });
    } else {
        out.refuseFmt(.limit, .memory_budget, "shard memory: {d} + {d} would pass its share {d}", .{ o.use, o.delta, o.share });
    }
}

const testing = std.testing;

test "gate: what a client writes is refusable, what frees or the node writes for itself never is" {
    // Every type has a class (the switch is exhaustive); these are the ones
    // whose class decides a refusal.
    for ([_]EntryType{ .kv_put, .kv_batch, .stream_append, .queue_enqueue, .ts_write, .group_create, .action_invoke, .processing_submit }) |t| try testing.expectEqual(Class.refusable, classOf(t));
    for ([_]EntryType{ .kv_delete, .kv_reap, .kv_expire, .stream_trim, .queue_deliver, .action_settle, .action_release, .namespace_create, .channel_inbound, .channel_unpark, .settings_budget_bind }) |t| try testing.expectEqual(Class.never, classOf(t));
    try testing.expectEqual(Class.own, classOf(.group_deliver));
    try testing.expectEqual(Class.own, classOf(.workflow_start));
    try testing.expectEqual(Class.own, classOf(.workflow_signal));
}

test "gate: nothing that adds nothing is held back, whatever the use" {
    try testing.expect(fits(1 << 40, 0, 1));
    try testing.expect(fits(1 << 40, -5, 1));
    try testing.expect(fits(90, 10, 100));
    try testing.expect(!fits(90, 11, 100));
    try testing.expect(!fits(std.math.maxInt(u64), 1, 100));
}

test "gate: the type-byte table agrees with classOf, and a byte no type has is not refusable" {
    for (0..256) |b| {
        const t = std.enums.fromInt(EntryType, @as(u8, @intCast(b))) orelse {
            try std.testing.expect(!REFUSABLE[b]);
            continue;
        };
        try std.testing.expectEqual(classOf(t) == .refusable, REFUSABLE[b]);
    }
}
