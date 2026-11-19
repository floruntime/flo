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
    /// A completion keeps 257 history events, one past the bound.
    runs_hist_keeps_257,
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
};

pub const active: Plant = std.meta.stringToEnum(Plant, @import("build_options").vopr_plant) orelse
    @compileError("-Dvopr-plant names no plant in src/plant.zig");
