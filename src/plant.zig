//! Undos of fixes the VOPR's regression seeds guard, one each, chosen at build
//! time with `-Dvopr-plant=<name>`. A seed only guards its fix if it fails with
//! the fix undone, and any change to what Raft sends moves every seed, so each
//! has to be checked again (`scripts/vopr-seed-undos.sh`). A site reads
//! `if (comptime plant.active == .name)`, so with none chosen it compiles out.

const std = @import("std");

pub const Plant = enum {
    none,
    /// The Raft log has no source below its ring.
    no_catch_up,
    /// A follower commits up to its own last index, not the last the RPC verified.
    commit_past_verified,
    /// Any ack frees a peer for its next batch, not only the latest batch's.
    any_ack_frees_peer,
    /// A member's asks go out, and the answers vouching for a config are dropped.
    no_vouches,
    /// `truncateAfter` leaves the ring's write cursor where it was.
    truncate_keeps_write_pos,
    /// The checker times a canonical entry from its first apply, not from when
    /// that replica learned it committed.
    erasure_from_apply,
    /// The simulator's async boot applies what its segments were sealed under.
    async_boot_applies,
    /// The simulator's repair waits on every core node, members or not.
    repair_waits_on_removed,
    /// Boot replay applies one entry past the commit watermark, so it can apply
    /// an entry a leader has not committed.
    replay_watermark_off_by_one,
    /// A workflow's redefinition enables it, whatever it was.
    defs_redefine_enables,
    /// Every workflow enable moves its schedule's anchor, not only one after
    /// a disable.
    defs_enable_moves_anchor,
    /// A cancelled run's end leaves its workflow's count of runs not ended.
    runs_cancel_keeps_active,
    /// A run's history flush sends its pending events but doesn't move
    /// past them, so its next chunk sends them again.
    runs_hist_flushed_kept,
    /// An ended run keeps its step outputs.
    runs_end_keeps_outputs,
    /// An action run's history flush sends its pending events but doesn't
    /// move past them.
    actions_hist_flushed_kept,
    /// An ended action run keeps its input.
    actions_end_keeps_input,
    /// A settle's resend, matching the first, tells the caller again.
    actions_resend_rewrites_result,
    /// An ended run takes a first step.
    runs_ended_takes_step,
    /// A schedule start's slot replaces the fired slot, even an earlier one.
    runs_fired_moves_back,
    /// A start with an idempotency key doesn't record it.
    runs_idem_unwritten,
    /// A start doesn't look its idempotency key up, so a second run starts.
    runs_idem_unchecked,
    /// A disabled workflow's child start creates its run.
    runs_disabled_child_created,
    /// A disabled workflow's child start is refused, which drops its
    /// parent's reply.
    runs_disabled_child_refused,
    /// Any start with a schedule's key fires its slot.
    runs_any_start_fires,
    /// Any start of a child's run tells its pending parent it started.
    runs_any_start_answers_parent,
    /// A step applies at any rev, not only its run's.
    runs_step_rev_unchecked,
    /// A client's start may carry a key only a producer's start may.
    runs_reserved_key_taken,
    /// A trigger's start read from an older making of its stream moves its
    /// place, like one from the stream made since.
    runs_trigger_made_unchecked,
    /// A trigger's start of an earlier event moves its place back.
    runs_trigger_place_moves_back,
    /// A trigger's start applies wherever the place logged is, not only right
    /// after the start before it.
    runs_trigger_order_unchecked,
    /// A child's start with no pending parent record creates its run.
    runs_child_without_parent_starts,
    /// A run's start is admitted past its shard's memory share.
    start_verdict_admits,
    /// A signal is admitted past its shard's memory share.
    signal_verdict_admits,
    /// A signal that wakes its run leaves the run's rev where it was.
    runs_signal_wake_keeps_rev,
    /// A step's takes leave the signals it took held.
    runs_take_keeps_signal,
    /// A run's end keeps the signals it holds.
    runs_end_keeps_signals,
    /// A run's signal bound counts every signal it was sent, taken or not.
    runs_signal_bound_counts_all,
    /// A step waiting on a signal its run holds applies.
    runs_stale_wait_unchecked,
    /// A wait takes the newest signal of its type, not the oldest.
    runs_take_newest,
    /// A signal of any type wakes a run waiting on a signal.
    runs_wake_any_type,
    /// A step's takes naming one type twice resolve to one signal.
    runs_take_ignores_picked,
    /// A signal to an ended run is held.
    runs_signal_after_end,
    /// A step takes its run's call's end before the call has ended.
    runs_settle_before_call_ends,
    /// A step leaves its run's call without taking its end.
    runs_call_left_unsettled,
    /// A step that takes its run's call's end keeps the call's record.
    runs_settled_call_kept,
    /// A call's run ending overwrites a call already settled refused or
    /// with no action.
    runs_done_overrides_settled,
    /// A KV delete still moves the value to the key's history.
    kv_delete_keeps_history,
    /// A KV reap keeps a key whose TTL ran out.
    kv_reap_keeps_expired,
    /// A KV reap leaves its namespace's version floor where it was, so a
    /// key's next life repeats its versions.
    kv_reap_floor_unraised,
    /// A KV reap drops every key it names, gone or not: it trusts the
    /// leader's clock over the entry's stamp.
    kv_reap_trusts_leader,
    /// The memory pool counts a family it exempts (workflow history).
    pool_counts_exempt_family,
    /// Records reach a store without its link, as a loader that skipped the
    /// store's counters would leave them.
    pool_load_bypasses_link,
    /// The memory gate takes a write past its shard's share.
    memory_admits_over_share,
    /// The memory gate refuses a queue's deliver, a duty, past the share.
    memory_refuses_system,
    /// A write's charged bytes count each key's first write, not its last.
    memory_delta_first_write,
    /// A group's deliver takes fresh records past its shard's share.
    group_deliver_uncapped,
    /// A workflow run's delete leaves its idempotency key.
    runs_delete_keeps_idem,
    /// A workflow run's delete takes a run that hasn't ended.
    runs_delete_takes_live,
    /// A history drop a repair skipped is neither sent again nor marked.
    history_drop_not_resent,
    /// An action run's delete sends no history drop.
    actions_delete_keeps_history,
    /// An action's counts don't move as its runs do.
    actions_counts_unmoved,
    /// A step applies to its run whatever incarnation it was decided for.
    runs_step_incarnation_unchecked,
    /// A run's delete leaves its place in its workflow's list.
    runs_delete_keeps_index,
    /// A child's start puts no place in the lists.
    runs_child_unindexed,
    /// A bulk delete takes a run that ended at or after its bound.
    runs_bulk_ignores_bound,
    /// A history fold's row counts only its own message, not the rows it folded.
    history_fold_total_dropped,
    /// A history drop takes its run's rows off nothing: usage keeps them.
    history_drop_unread,
    /// A workflow run's end keeps its run-history reserve.
    quota_end_keeps_reserve,
    /// An action run's end keeps its run-history reserve.
    quota_action_end_keeps_reserve,
    /// A workflow start is admitted past its namespace's quota.
    quota_start_admits,
    /// An action invoke, direct or a call's, is admitted past the quota.
    quota_invoke_admits,
    /// A history message a repair skipped leaves its bytes untaken: its
    /// namespace's in-flight history counts it until the trim.
    history_skip_untaken,
    /// A workflow run reserves nothing for what its history may still add
    /// past the part: its step in flight, its wake and its end go uncounted.
    quota_reserve_omits_step,
};

pub const active: Plant = std.meta.stringToEnum(Plant, @import("build_options").vopr_plant) orelse
    @compileError("-Dvopr-plant names no plant in src/plant.zig");
