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
};

pub const active: Plant = std.meta.stringToEnum(Plant, @import("build_options").vopr_plant) orelse
    @compileError("-Dvopr-plant names no plant in src/plant.zig");
